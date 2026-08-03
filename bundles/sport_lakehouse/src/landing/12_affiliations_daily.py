# Databricks notebook source
# 12_affiliations_daily — create affiliations for new members + simulate age-weighted transfers
# Writes: affiliations

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

TABLE = "affiliations"

rnd = random.Random(seed_for(TABLE))

members = spark.table(tbl("members")).select("member_id","address_id","birth_date","created_at")
addresses = spark.table(tbl("addresses")).select("address_id","municipality_name","county_name")
clubs = spark.table(tbl("clubs")).select("club_id","address_id")
club_geo = clubs.join(addresses, on="address_id", how="left") \
               .select("club_id","municipality_name","county_name")

# helper: age at RUN_DATE
members_w_geo = (members.join(addresses, on="address_id", how="left")
                        .withColumn("age", F.floor(F.datediff(F.lit(RUN_DATE), F.col("birth_date"))/F.lit(365.25)))
                        .select("member_id","municipality_name","county_name","age","created_at"))

# Existing active affiliations
active_aff = spark.table(tbl("affiliations")).where(F.col("end_date").isNull()) \
              .select("member_id","club_id","affiliation_id")

# COMMAND ----------

# DBTITLE 1,Cell 5
# 1) New members today → ensure an active affiliation
df_new_affiliations = None  # Initialize at notebook level

new_members_today = members_w_geo.where(F.col("created_at") == F.lit(RUN_DATE)) \
                                .join(active_aff.select("member_id").distinct(), on="member_id", how="left_anti")

# build muni/county → clubs map (small enough)
clubs_by_muni = {r["municipality_name"]: r["club_ids"] for r in club_geo.groupBy("municipality_name")
                                                     .agg(F.collect_list("club_id").alias("club_ids")).collect()}
clubs_by_county = {r["county_name"]: r["club_ids"] for r in club_geo.groupBy("county_name")
                                                       .agg(F.collect_list("club_id").alias("club_ids")).collect()}
all_club_ids = [r["club_id"] for r in club_geo.select("club_id").distinct().collect()]

def pick_club(muni, county, r):
    if muni in clubs_by_muni and r.random() < 0.80:
        return int(r.choice(clubs_by_muni[muni]))
    if county in clubs_by_county and r.random() < 0.85:
        return int(r.choice(clubs_by_county[county]))
    return int(r.choice(all_club_ids))

rows_new=[]
for r in new_members_today.collect():
    mid = int(r["member_id"])
    rr = random.Random(seed_for(TABLE) + mid)
    club_id = pick_club(r["municipality_name"], r["county_name"], rr)
    aff_id = mid  # stable first affiliation
    ts = datetime.combine(RUN_DATE, datetime.min.time()) + timedelta(hours=rr.randint(8,20), minutes=rr.randint(0,59))
    rows_new.append({
        "affiliation_id": int(aff_id),
        "member_id": mid,
        "club_id": int(club_id),
        "start_date": RUN_DATE,
        "end_date": None,
        "reason": "new_member",
        "created_at": ts,
        "updated_at": ts
    })

if not rows_new:
    df_new = None
else:
    # Use the actual table schema to avoid PySpark type-inference issues
    aff_schema = spark.table(tbl("affiliations")).schema
    cols = [f.name for f in aff_schema.fields]

    def _normalize_row(d):
        """Return a tuple of values in the same order as `cols`,
           casting datetimes/dates where appropriate."""
        out = []
        for f in aff_schema.fields:
            v = d.get(f.name, None)
            if v is None:
                out.append(None)
                continue

            dt = f.dataType.simpleString()
            # keep Python datetime for timestamp columns, convert if necessary
            if ("timestamp" in dt) and isinstance(v, datetime):
                out.append(v)
            elif ("date" in dt) and isinstance(v, datetime):
                out.append(v.date())
            else:
                out.append(v)
        return tuple(out)

    rows_tuples = [_normalize_row(r) for r in rows_new]
    df_new = spark.createDataFrame(rows_tuples, schema=aff_schema)
    df_new_affiliations = df_new  # Store at notebook level for Cell 7

if df_new is not None and df_new.count() > 0:
    merge_into("affiliations", df_new, ["affiliation_id"])
    display(df_new.limit(20))
else:
    print("No new affiliations to create today.")

# COMMAND ----------

# DBTITLE 1,Cell 6
# 2) Transfers: small daily share, age-weighted, exclude <16
df_tr_affiliations = None  # Initialize at notebook level
rows_tr = []  # Initialize at notebook level

# Base transfer rate: 0.10%–0.30% scaled by volume
base_rate = rnd.uniform(0.001, 0.003) * VOLUME_MULT
base_rate = min(0.02, base_rate)  # cap 2%

eligible = (members_w_geo.where(F.col("age") >= F.lit(16))
                       .join(active_aff, on="member_id", how="inner")
                       .select("member_id","age","municipality_name","county_name","club_id"))

eligible_cnt = eligible.count()
n_transfer = int(eligible_cnt * base_rate)

if n_transfer <= 0:
    print("No transfers today (n_transfer=0).")
else:
    # weight by age buckets
    def weight(a):
        if 18 <= a <= 35: return 2.2
        if 16 <= a <= 17: return 0.3
        if 36 <= a <= 60: return 1.0
        return 0.4

    elig_rows = eligible.collect()
    # build weighted list of member_ids deterministically
    rr = random.Random(seed_for(TABLE) + 999)
    weighted = []
    for x in elig_rows:
        weighted.append((int(x["member_id"]), weight(int(x["age"]))))

    # sample without replacement by weights
    chosen = set()
    for _ in range(min(n_transfer, len(weighted))):
        total = sum(w for _, w in weighted if _ not in chosen)
        if total <= 0:
            break
        pick = rr.random() * total
        s = 0.0
        for mid, w in weighted:
            if mid in chosen:
                continue
            s += w
            if s >= pick:
                chosen.add(mid)
                break

    if not chosen:
        print("No transfers selected after weighting.")
    else:
        # Prepare updates: end old affiliation
        chosen_list = list(chosen)
        chosen_df = spark.createDataFrame([(int(m),) for m in chosen_list], ["member_id"])
        # end_date = RUN_DATE - 1 for current active
        spark.sql(f"""
          UPDATE {tbl("affiliations")}
          SET end_date = DATE('{(RUN_DATE - timedelta(days=1)).isoformat()}'),
              updated_at = current_timestamp(),
              reason = CASE WHEN reason IS NULL OR reason = '' THEN 'transfer' ELSE reason END
          WHERE end_date IS NULL AND member_id IN ({','.join([str(m) for m in chosen_list])})
        """)

        # Capture just-closed affiliations so bronze sees the end_date change
        df_closed_affiliations = spark.table(tbl("affiliations")).where(
            F.col("member_id").isin(chosen_list)
        ).where(F.col("end_date") == F.lit(RUN_DATE - timedelta(days=1)))

        # Insert new affiliations for transfers
        for x in elig_rows:
            mid = int(x["member_id"])
            if mid not in chosen:
                continue
            rr2 = random.Random(seed_for(TABLE) + mid + int(RUN_DATE.strftime("%Y%m%d")))
            cur_club = int(x["club_id"])
            new_club = pick_club(x["municipality_name"], x["county_name"], rr2)
            if new_club == cur_club and len(all_club_ids) > 1:
                # pick any other
                new_club = int(rr2.choice([c for c in all_club_ids if int(c) != cur_club]))
            aff_id = mid*100_000_000 + int(RUN_DATE.strftime("%Y%m%d"))  # deterministic transfer id
            ts = datetime.combine(RUN_DATE, datetime.min.time()) + timedelta(hours=rr2.randint(8,20), minutes=rr2.randint(0,59))
            rows_tr.append({
                "affiliation_id": int(aff_id),
                "member_id": mid,
                "club_id": int(new_club),
                "start_date": RUN_DATE,
                "end_date": None,
                "reason": "transfer",
                "created_at": ts,
                "updated_at": ts
            })

# Create DataFrame and merge (outside nested if blocks)
if rows_tr:
    aff_schema = spark.table(tbl("affiliations")).schema
    cols = [f.name for f in aff_schema.fields]

    rows_tuples = [
        tuple(r.get(c, None) for c in cols)
        for r in rows_tr
    ]

    df_tr = spark.createDataFrame(rows_tuples, schema=aff_schema)
    df_tr_affiliations = df_tr  # Store at notebook level for Cell 7
    
    merge_into("affiliations", df_tr, ["affiliation_id"])
    print(f"Transfers created: {df_tr.count()}")
    display(df_tr.limit(20))
else:
    print("No transfers to process.")
    df_closed_affiliations = None

# COMMAND ----------

# DBTITLE 1,Cell 7
# Combine: new members + closed (transferred-out) + new transfer affiliations
# Closed affiliations must reach bronze so silver SCD2 captures the end_date change.
dfs = [df for df in [df_new_affiliations, df_closed_affiliations, df_tr_affiliations] if df is not None]
df_combined = None
for df in dfs:
    df_combined = df if df_combined is None else df_combined.unionByName(df)

export_to_landing(TABLE, df_combined)