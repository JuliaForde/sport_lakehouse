# Databricks notebook source
# 14_memberships_daily — create memberships for new members + expire/renew memberships
# Writes: memberships

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

TABLE = "memberships"
rnd = random.Random(seed_for(TABLE))

members = spark.table(tbl("members")).select("member_id","birth_date","created_at")
mem_today = members.where(F.col("created_at") == F.lit(RUN_DATE)).select("member_id","birth_date","created_at")

existing_active = spark.table(tbl("memberships")).where(F.col("status") == F.lit("active")) \
                    .select("member_id","membership_id","end_date","membership_type","price_nok")

def age_on(d, b):
    return int((d - b).days // 365.25)

def membership_type_for_age(a, r):
    if a < 19: return "youth"
    if a < 67: return "adult"
    return "senior"

def price_for_type(t, r):
    base = {"trial": 0, "youth": 550, "adult": 1250, "senior": 850, "family": 2200}.get(t, 1200)
    return int(max(0, r.gauss(base, base*0.08)))

def membership_id(mid: int, start_date) -> int:
    return int(mid)*100_000_000 + int(start_date.strftime("%Y%m%d"))

# COMMAND ----------

# 1) Expire memberships whose end_date < RUN_DATE
# Capture IDs BEFORE the UPDATE — updated_at will be current_timestamp(), not RUN_DATE,
# so filtering by updated_at after the fact breaks historical re-runs.
_expire_condition = f"status = 'active' AND end_date < DATE('{RUN_DATE.isoformat()}')"
_expiring_ids = [int(r["membership_id"]) for r in
                 spark.table(tbl("memberships")).where(_expire_condition).select("membership_id").collect()]

spark.sql(f"""
  UPDATE {tbl("memberships")}
  SET status = 'expired',
      updated_at = current_timestamp()
  WHERE {_expire_condition}
""")

# Read back the just-expired rows by ID — safe for both live runs and historical re-runs
df_expired_memberships = None
if _expiring_ids:
    df_expired_memberships = spark.table(tbl("memberships")).where(
        F.col("membership_id").isin(_expiring_ids)
    )

# COMMAND ----------

# DBTITLE 1,Cell 6
# 2) Create memberships for new members (if none active)
df_new_memberships = None  # Initialize at notebook level

# left anti join to avoid duplicates
new_without_active = (mem_today.join(existing_active.select("member_id").distinct(), on="member_id", how="left_anti"))

rows=[]
for r0 in new_without_active.collect():
    mid = int(r0["member_id"])
    bdate = r0["birth_date"]
    rr = random.Random(seed_for(TABLE) + mid)
    a = age_on(RUN_DATE, bdate)
    # trial for a small share of 13+ only
    mtype = "trial" if (a >= 13 and rr.random() < 0.10) else membership_type_for_age(a, rr)
    if mtype in ("adult","senior") and rr.random() < 0.05:
        mtype = "family"
    duration = 30 if mtype == "trial" else 365
    end_date = RUN_DATE + timedelta(days=duration)
    price = price_for_type(mtype, rr)
    ts = datetime.combine(RUN_DATE, datetime.min.time()) + timedelta(hours=rr.randint(8,20), minutes=rr.randint(0,59))
    rows.append({
        "membership_id": membership_id(mid, RUN_DATE),
        "member_id": mid,
        "membership_type": mtype,
        "status": "active",
        "start_date": RUN_DATE,
        "end_date": end_date,
        "price_nok": int(price),
        "created_at": ts,
        "updated_at": ts
    })

if rows:
    df_new = spark.createDataFrame(rows)
    df_new_memberships = df_new  # Store at notebook level for Cell 8
    merge_into("memberships", df_new, ["membership_id"])
    display(df_new.limit(20))
else:
    print("No new memberships to create today.")

# COMMAND ----------

# DBTITLE 1,Cell 7
# 3) Renewals (some expiring yesterday renew today)
df_renew_memberships = None  # Initialize at notebook level

# Choose memberships ended yesterday that are expired now
yesterday = RUN_DATE - timedelta(days=1)

ended_yesterday = (spark.table(tbl("memberships"))
                    .where(F.col("end_date") == F.lit(yesterday))
                    .where(F.col("status") == F.lit("expired"))
                    .select("member_id","membership_type"))

if ended_yesterday.count() == 0:
    print("No memberships ended yesterday.")
else:
    members_map = {r["member_id"]: r["birth_date"] for r in members.select("member_id","birth_date").collect()}
    renew_rows=[]
    for r1 in ended_yesterday.collect():
        mid = int(r1["member_id"])
        rr = random.Random(seed_for(TABLE) + mid + 777)
        # renewal probability depends on previous type
        prev = (r1["membership_type"] or "")
        p = 0.80 if prev in ("adult","youth") else (0.74 if prev == "senior" else 0.65)
        # trials convert to normal membership with higher chance
        if prev == "trial":
            p = 0.62
        if rr.random() > p:
            continue

        bdate = members_map.get(mid)
        a = age_on(RUN_DATE, bdate) if bdate else 30
        mtype = membership_type_for_age(a, rr) if prev == "trial" else prev
        if mtype in ("adult","senior") and rr.random() < 0.05:
            mtype = "family"
        end_date = RUN_DATE + timedelta(days=365)
        price = price_for_type(mtype, rr)
        ts = datetime.combine(RUN_DATE, datetime.min.time()) + timedelta(hours=rr.randint(8,20), minutes=rr.randint(0,59))
        renew_rows.append({
            "membership_id": membership_id(mid, RUN_DATE),
            "member_id": mid,
            "membership_type": mtype,
            "status": "active",
            "start_date": RUN_DATE,
            "end_date": end_date,
            "price_nok": int(price),
            "created_at": ts,
            "updated_at": ts
        })

    if renew_rows:
        df_renew = spark.createDataFrame(renew_rows)
        df_renew_memberships = df_renew  # Store at notebook level for Cell 8
        merge_into("memberships", df_renew, ["membership_id"])
        print(f"Renewals inserted: {df_renew.count()}")
        display(df_renew.limit(20))
    else:
        print("No renewals created today.")

# COMMAND ----------

# DBTITLE 1,Cell 8
# Combine: new + renewals + just-expired (status change must reach bronze)
dfs = [df for df in [df_new_memberships, df_renew_memberships, df_expired_memberships] if df is not None]
df_combined = None
for df in dfs:
    df_combined = df if df_combined is None else df_combined.unionByName(df)

export_to_landing(TABLE, df_combined)