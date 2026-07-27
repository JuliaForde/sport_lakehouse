# Databricks notebook source
# 40_results_daily — generate results for competitions ended in last 1–3 days (late arrivals) + small corrections
# Writes: results (MERGE by result_id = participation_id)

# COMMAND ----------

from datetime import date

dbutils.widgets.text("run_date",date.today().isoformat() )          # <- Monday
dbutils.widgets.text("volume", "medium")                # low|medium|high
dbutils.widgets.text("volume_factor", "1.0")            # scales volume_mult

# COMMAND ----------

# MAGIC %run ./00_utils

# COMMAND ----------

# DBTITLE 1,Cell 4
import random
from datetime import timedelta, datetime
from pyspark.sql import functions as F

TABLE = "results"
r = random.Random(seed_for(TABLE))
df_new_results = None  # Initialize at notebook level

# Explicit schema — corrected_at is always None (and score/time_seconds can be
# all-None per batch), so we must NOT let Spark infer types from the dicts.
from pyspark.sql.types import StructType, StructField, LongType, IntegerType, StringType, BooleanType, TimestampType
RESULTS_SCHEMA = StructType([
    StructField("result_id",        LongType(),      False),
    StructField("participation_id", LongType(),      False),
    StructField("position",         IntegerType(),   True),
    StructField("score",            IntegerType(),   True),
    StructField("time_seconds",     IntegerType(),   True),
    StructField("notes",            StringType(),    True),
    StructField("is_official",      BooleanType(),   True),
    StructField("recorded_at",      TimestampType(), True),
    StructField("corrected_at",     TimestampType(), True),
])

# Competitions ended in last 3 days and completed
ended_from = RUN_DATE - timedelta(days=3)
ended_to = RUN_DATE - timedelta(days=1)

comps = (spark.table(tbl("competitions"))
          .where(F.col("status") == F.lit("completed"))
          .where(F.col("end_date") >= F.lit(ended_from))
          .where(F.col("end_date") <= F.lit(ended_to))
          .select("competition_id","sport_type_id","end_date")
          .collect())

if not comps:
    print(f"No completed competitions ended between {ended_from} and {ended_to}.")
else:
    sport_map = {r0["sport_type_id"]: r0["sport_type_name"] for r0 in spark.table(tbl("sport_types")).select("sport_type_id","sport_type_name").collect()}
    # participation eligible
    parts = spark.table(tbl("participation")).select("participation_id","competition_id","status")

    existing = set([int(x["result_id"]) for x in spark.table(tbl("results")).select("result_id").collect()])

    def sport_result(sport_name: str, rr):
        if sport_name in ["cross-country skiing","alpine skiing","athletics","cycling","orienteering","biathlon"]:
            base = rr.uniform(600, 7200)
            noise = rr.uniform(-base*0.12, base*0.12)
            t = max(60, int(base + noise))
            return None, t
        score = rr.randint(0, 100) if sport_name=="basketball" else rr.randint(0, 15)
        return score, None

    rows=[]
    for c in comps:
        comp_id = int(c["competition_id"])
        sport_name = sport_map.get(int(c["sport_type_id"]), "unknown")
        end_date = c["end_date"]
        rr = random.Random(seed_for(TABLE) + comp_id)

        eligible = (parts.where(F.col("competition_id") == F.lit(comp_id))
                         .where(F.col("status").isin(["checked_in","registered"]))
                         .select("participation_id")
                         .collect())
        pids = [int(x["participation_id"]) for x in eligible if int(x["participation_id"]) not in existing]
        if not pids:
            continue

        # only generate for ~85% starters
        rr.shuffle(pids)
        pids = pids[:max(1, int(len(pids)*0.85))]

        scored=[]
        for pid in pids:
            score, tsec = sport_result(sport_name, rr)
            note = None if rr.random() < 0.93 else rr.choice(["photo finish","wind aided","course short","equipment issue","injury"])
            scored.append((pid, score, tsec, note))

        # rank
        if sport_name in ["cross-country skiing","alpine skiing","athletics","cycling","orienteering","biathlon"]:
            scored.sort(key=lambda x: x[2] if x[2] is not None else 9_999_999)
        else:
            scored.sort(key=lambda x: -(x[1] if x[1] is not None else -1))

        is_official = ((RUN_DATE - end_date).days > 7)
        recorded_at = datetime.combine(RUN_DATE, datetime.min.time()) + timedelta(hours=rr.randint(10,22))

        for pos, (pid, score, tsec, note) in enumerate(scored, start=1):
            rows.append({
                "result_id": int(pid),  # deterministic
                "participation_id": int(pid),
                "position": int(pos),
                "score": int(score) if score is not None else None,
                "time_seconds": int(tsec) if tsec is not None else None,
                "notes": note,
                "is_official": bool(is_official),
                "recorded_at": recorded_at,
                "corrected_at": None
            })
            existing.add(int(pid))

    if rows:
        df_new = spark.createDataFrame(rows, schema=RESULTS_SCHEMA)
        df_new_results = df_new  # Store at notebook level for Cell 6
        merge_into("results", df_new, ["result_id"])
        print(f"Inserted results: {len(rows)}")
        display(df_new.limit(20))
    else:
        print("No new results inserted (maybe already generated).")

# COMMAND ----------

# DBTITLE 1,Cell 5
# --------------------- Final safe upsert for results ---------------------
from pyspark.sql import functions as F

df_final_results = None  # Initialize at notebook level for Cell 6

# Determine which DataFrame exists: prefer df_new (created earlier in this notebook)
df_base = None
if 'df_new' in globals():
    df_base = df_new
elif 'df_out' in globals():
    df_base = df_out
elif 'df_results' in globals():
    df_base = df_results
else:
    df_base = None

if df_base is None:
    print("No results DataFrame found (df_new / df_out / df_results). Nothing to merge.")
else:
    # Ensure expected columns exist (adapt if your column names differ)
    expected_cols = {"result_id", "participation_id"}
    missing = expected_cols - set(df_base.columns)
    if missing:
        raise Exception(f"Results DataFrame is missing expected columns: {missing}")

    # deterministic seed base (integer) — use seed_for helper for reproducibility
    SEED_RESULTS = seed_for("results_daily_seed", run_date=RUN_DATE)

    # Deterministic offsets using hash(participation_id, SEED)
    # score_offset -> -1,0,+1  (range size 3)
    score_offset_expr = (F.abs(F.hash(F.col("participation_id"), F.lit(SEED_RESULTS))) % F.lit(3)) - F.lit(1)
    # time_offset -> -4 .. +4  (range size 9)
    time_offset_expr  = (F.abs(F.hash(F.col("participation_id"), F.lit(SEED_RESULTS + 7))) % F.lit(9)) - F.lit(4)

    # Compute adjusted columns deterministically and materialize recorded_at once
    df_final = (
        df_base
        .withColumn(
            "score_adj",
            F.when(F.col("score").isNotNull(),
                   F.greatest(F.lit(0), (F.col("score") + score_offset_expr).cast("int"))
                  ).otherwise(F.col("score"))
        )
        .withColumn(
            "time_seconds_adj",
            F.when(F.col("time_seconds").isNotNull(),
                   F.greatest(F.lit(1), (F.col("time_seconds") + time_offset_expr).cast("int"))
                  ).otherwise(F.col("time_seconds"))
        )
        .withColumn("recorded_at", F.current_timestamp())
    )

    # Build the output column list by NAME (avoid c._jc — unsupported on serverless).
    # Output = the original result columns, with score/time_seconds replaced by the
    # adjusted values, result_id/participation_id first. (Intermediate *_adj cols dropped.)
    orig_cols = list(df_base.columns)
    ordered_names = ["result_id", "participation_id"] + [c for c in orig_cols if c not in ("result_id", "participation_id")]

    def _out_col(name):
        if name == "score":
            return F.col("score_adj").alias("score")
        if name == "time_seconds":
            return F.col("time_seconds_adj").alias("time_seconds")
        return F.col(name)

    df_out_final = df_final.select(*[_out_col(n) for n in ordered_names])

    # Materialize values so no nondeterministic exprs remain for the merge
    df_out_final = df_out_final
    _ = df_out_final.count()   # force evaluation
    
    df_final_results = df_out_final  # Store at notebook level for Cell 6

    # Upsert into results table
    merge_into("results", df_out_final, ["result_id"])
    print(f"Upserted {df_out_final.count()} result rows for {RUN_DATE.isoformat()}")
# -----------------------------------------------------------------------

# COMMAND ----------

# DBTITLE 1,Cell 6
# Use df_final_results (adjusted, Cell 5) if it exists, else df_new_results (Cell 4),
# then export via the shared helper (overwrite per run_date partition — idempotent).
df_to_export = df_final_results if df_final_results is not None else df_new_results
export_to_landing(TABLE, df_to_export)