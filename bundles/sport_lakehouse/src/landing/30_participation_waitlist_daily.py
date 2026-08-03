# Databricks notebook source
# 30_participation_waitlist_daily — daily registrations + cancellations + waitlist promotions (capacity-aware)
# Writes: participation, competition_waitlist

# COMMAND ----------

from datetime import date

dbutils.widgets.text("run_date",date.today().isoformat() )          # <- Monday
dbutils.widgets.text("volume", "medium")                # low|medium|high
dbutils.widgets.text("volume_factor", "1.0")            # scales volume_mult

# COMMAND ----------

# MAGIC %run ./00_utils

# COMMAND ----------

import random
from datetime import timedelta, datetime
from pyspark.sql import functions as F

r = random.Random(seed_for("participation"))

# Look at upcoming competitions within next 14 days
comps = (spark.table(tbl("competitions")).alias("c")
          .where(F.col("status") == F.lit("scheduled"))
          .where(F.col("start_date") >= F.lit(RUN_DATE))
          .where(F.col("start_date") <= F.lit(RUN_DATE + timedelta(days=14)))
          .join(spark.table(tbl("sport_types")).select("sport_type_id","popularity_weight","sport_mode").alias("s"),
                on="sport_type_id", how="left")
          .select("competition_id","sport_type_id","level","start_date","address_id","capacity","registration_deadline",
                  "popularity_weight","sport_mode")
          .collect())

if not comps:
    dbutils.notebook.exit("No upcoming competitions in the next 14 days; nothing to register.")
else:
    members = spark.table(tbl("members")).select("member_id","birth_date","address_id")
    addresses = spark.table(tbl("addresses")).select("address_id","municipality_name","county_name")
    memberships = spark.table(tbl("memberships")).where(F.col("status") == F.lit("active")).select("member_id").distinct()
    affiliations = spark.table(tbl("affiliations")).where(F.col("end_date").isNull()).select("member_id","club_id")

    mem_geo = (members.join(addresses, on="address_id", how="left")
                     .join(memberships, on="member_id", how="inner")
                     .join(affiliations, on="member_id", how="left")
                     .select("member_id","birth_date","municipality_name","county_name","club_id"))

    # existing participation and waitlist
    part = spark.table(tbl("participation")).select("competition_id","member_id","status")
    wait = spark.table(tbl("competition_waitlist")).select("competition_id","member_id","status","waitlist_id")

    # per-competition current counts
    part_active = (spark.table(tbl("participation"))
                    .where(F.col("status") != F.lit("cancelled"))
                    .groupBy("competition_id")
                    .agg(F.count("*").alias("n_part")))
    wait_active = (spark.table(tbl("competition_waitlist"))
                    .where(F.col("status") == F.lit("waiting"))
                    .groupBy("competition_id")
                    .agg(F.count("*").alias("n_wait")))

# COMMAND ----------

    # 1) Small cancellations for comps starting soon (free up some capacity)
    # Cancel 0.2%–0.8% of registered for competitions within 7 days (scaled by volume)
    cancel_rate = min(0.03, max(0.002, r.uniform(0.002, 0.008) * VOLUME_MULT))

    soon_ids = [int(c["competition_id"]) for c in comps if (c["start_date"] - RUN_DATE).days <= 7]
    if soon_ids:
        # sample some registrations to cancel
        cand = (spark.table(tbl("participation"))
                  .where(F.col("competition_id").isin(soon_ids))
                  .where(F.col("status") == F.lit("registered"))
                  .select("participation_id","competition_id","member_id")
                  .orderBy(F.rand(seed_for("cancel") ))
                  .limit(int(50000 * VOLUME_MULT)))
        cand_rows = cand.collect()
        n_cancel = int(len(cand_rows) * cancel_rate)
        cancel_ids = [int(x["participation_id"]) for x in cand_rows[:n_cancel]]
        if cancel_ids:
            spark.sql(f"""
              UPDATE {tbl("participation")}
              SET status = 'cancelled',
                  status_updated_at = current_timestamp()
              WHERE participation_id IN ({",".join(map(str, cancel_ids))})
            """)
            print(f"Cancelled {len(cancel_ids)} registrations.")
            # Capture just-cancelled rows so bronze sees the status change
            df_cancelled_participation = spark.table(tbl("participation")).where(
                F.col("participation_id").isin(cancel_ids)
            ).where(F.col("status") == "cancelled")
        else:
            print("No cancellations applied today.")
            df_cancelled_participation = None
    else:
        print("No competitions within 7 days to apply cancellations.")
        df_cancelled_participation = None

# COMMAND ----------

# DBTITLE 1,Cell 6
    # 2) Promote from waitlist if capacity available
    # For comps in window, promote up to freed spots
    df_promoted_participation = None  # Initialize at notebook level
    
    counts = (spark.table(tbl("competitions")).select("competition_id","capacity")
                .join(part_active, on="competition_id", how="left")
                .join(wait_active, on="competition_id", how="left")
                .fillna({"n_part":0,"n_wait":0})
                .where(F.col("competition_id").isin([int(c["competition_id"]) for c in comps]))
                .withColumn("free_spots", F.col("capacity") - F.col("n_part"))
                .where(F.col("free_spots") > F.lit(0))
                .collect())

    promote_rows=[]
    promote_updates=[]
    # allocate participation ids for promotions
    # We'll use state allocator for participation ids
    proposed_promotes = sum(int(x["free_spots"]) for x in counts)
    alloc_p = allocate_ids("participation", "participation_id", int(proposed_promotes))
    p_start = alloc_p["start_id"]
    p_next = p_start

    for c in counts:
        comp_id = int(c["competition_id"])
        free = int(c["free_spots"])
        if free <= 0:
            continue
        # pick earliest waiting
        wl = (spark.table(tbl("competition_waitlist"))
                .where(F.col("competition_id") == F.lit(comp_id))
                .where(F.col("status") == F.lit("waiting"))
                .orderBy(F.col("added_at").asc())
                .limit(free)
                .select("waitlist_id","member_id","club_id","added_at")
                .collect())
        for w in wl:
            p_next += 1
            promote_rows.append({
                "participation_id": int(p_next),
                "competition_id": comp_id,
                "member_id": int(w["member_id"]),
                "club_id": int(w["club_id"]) if w["club_id"] is not None else None,
                "registered_at": datetime.combine(RUN_DATE, datetime.min.time()) + timedelta(hours=12),
                "status": "registered",
                "status_updated_at": datetime.combine(RUN_DATE, datetime.min.time()) + timedelta(hours=12),
                "source": "waitlist_promo",
                "bib_number": None
            })
            promote_updates.append((int(w["waitlist_id"]), int(p_next)))

    if promote_rows:
        df_prom = spark.createDataFrame(promote_rows, schema=spark.table(tbl("participation")).schema)
        df_promoted_participation = df_prom  # Store at notebook level for Cell 8
        merge_into("participation", df_prom, ["participation_id"])
        # update waitlist rows to promoted
        ids = ",".join(str(x[0]) for x in promote_updates)
        spark.sql(f"""
          UPDATE {tbl("competition_waitlist")}
          SET status = 'promoted',
              updated_at = current_timestamp()
          WHERE waitlist_id IN ({ids})
        """)
        print(f"Promoted from waitlist: {len(promote_rows)}")

        # set promoted_participation_id (best-effort; update per row)
        # (small set, so ok)
        for wid, pid in promote_updates[:10000]:
            spark.sql(f"""
              UPDATE {tbl("competition_waitlist")}
              SET promoted_participation_id = {pid}
              WHERE waitlist_id = {wid}
            """)
        # Capture just-promoted waitlist rows so bronze sees the status change
        promoted_wl_ids = [x[0] for x in promote_updates]
        df_promoted_waitlist = spark.table(tbl("competition_waitlist")).where(
            F.col("waitlist_id").isin(promoted_wl_ids)
        ).where(F.col("status") == "promoted")
    else:
        print("No waitlist promotions today.")
        df_promoted_waitlist = None

# COMMAND ----------

# DBTITLE 1,Cell 7
# Initialize at notebook level for Cell 8
df_new_participation = None
df_new_waitlist = None

# Try to recover variables from globals() (be tolerant if some names didn't get created)
rows_part = globals().get("rows_part", []) or []
rows_wait = globals().get("rows_wait", []) or []

alloc_part = globals().get("alloc_part", None)
alloc_wait = globals().get("alloc_wait", None)
part_start = globals().get("part_start", None)
wait_start = globals().get("wait_start", None)

# Helper to convert list-of-dicts to DataFrame using the target table schema
def _rows_to_df(rows_list, target_table):
    if not rows_list:
        return None
    # Use the table's schema to avoid Spark Connect inference issues
    target_schema = spark.table(tbl(target_table)).schema
    cols = [f.name for f in target_schema.fields]
    rows_tuples = [tuple(r.get(c, None) for c in cols) for r in rows_list]
    return spark.createDataFrame(rows_tuples, schema=target_schema)

# Participation inserts
if rows_part:
    df_p = _rows_to_df(rows_part, "participation")
    if df_p is None:
        print("Participation: nothing to upsert after schema normalization.")
    else:
        df_new_participation = df_p  # Store at notebook level for Cell 8
        merge_into("participation", df_p, ["participation_id"])
        # Only record allocation if alloc_part exists and indicates first run
        if alloc_part is not None and not alloc_part.get("already_ran", False) and part_start is not None:
            try:
                record_state("participation",
                             part_start,
                             part_start + alloc_part["n_rows"],
                             alloc_part["n_rows"],
                             notes="Allocated for new regs/promos")
            except Exception as e:
                print(f"Warning: record_state for participation failed: {e}")
        print(f"Participation upserted rows: {df_p.count()}")
else:
    print("No participation rows to insert (rows_part not present or empty).")

# Waitlist inserts
if rows_wait:
    df_w = _rows_to_df(rows_wait, "competition_waitlist")
    if df_w is None:
        print("Waitlist: nothing to upsert after schema normalization.")
    else:
        df_new_waitlist = df_w  # Store at notebook level for Cell 8
        merge_into("competition_waitlist", df_w, ["waitlist_id"])
        if alloc_wait is not None and not alloc_wait.get("already_ran", False) and wait_start is not None:
            try:
                record_state("competition_waitlist",
                             wait_start,
                             wait_start + alloc_wait["n_rows"],
                             alloc_wait["n_rows"],
                             notes="Allocated waitlist ids")
            except Exception as e:
                print(f"Warning: record_state for competition_waitlist failed: {e}")
        print(f"Waitlist upserted rows: {df_w.count()}")
else:
    print("No waitlist rows to insert (rows_wait not present or empty).")

print(f"List lengths -> participation: {len(rows_part)} | waitlist: {len(rows_wait)}")

# COMMAND ----------

# DBTITLE 1,Cell 8
# Export participation and waitlist via the shared helper
# (overwrite per run_date partition — idempotent).

# --- Participation: new registrations + promoted from waitlist + just-cancelled ---
part_dfs = [df for df in [df_new_participation, df_promoted_participation, df_cancelled_participation] if df is not None]
df_participation_combined = None
for df in part_dfs:
    df_participation_combined = df if df_participation_combined is None else df_participation_combined.unionByName(df)

# --- Waitlist: new entries + just-promoted (status change must reach bronze) ---
wl_dfs = [df for df in [df_new_waitlist, df_promoted_waitlist] if df is not None]
df_waitlist_combined = None
for df in wl_dfs:
    df_waitlist_combined = df if df_waitlist_combined is None else df_waitlist_combined.unionByName(df)

export_to_landing("participation", df_participation_combined)
export_to_landing("competition_waitlist", df_waitlist_combined)