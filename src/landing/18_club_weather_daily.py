# Databricks notebook source
# 18_club_weather_daily — daily weather observations for monitored clubs (integrated with 00_utils)
#
# Fixes vs original:
# - Uses `%run ./00_utils` (no illegal `from 00_utils import *`)
# - Uses shared widgets: run_date / volume / volume_factor (exposed as RUN_DATE, VOLUME, VOLUME_MULT)
# - Keeps monitoring table creation logic
# - Uses merge_into() for idempotent reruns (key = weather_date + club_id)

# COMMAND ----------

from datetime import date

dbutils.widgets.text("run_date",date.today().isoformat() )          # <- Monday
dbutils.widgets.text("volume", "medium")                # low|medium|high
dbutils.widgets.text("volume_factor", "1.0")            # scales volume_mult

# COMMAND ----------

# MAGIC %run ./00_utils

# COMMAND ----------

from pyspark.sql import functions as F
import random, math
from datetime import datetime

TABLE_DAILY = "club_weather_daily"
TABLE_MON   = "club_weather_monitoring"
MONITOR_RATE = 0.785  # ~78.5% coverage

print(f"RUN_DATE={RUN_DATE.isoformat()}  VOLUME={VOLUME}  VOLUME_MULT={VOLUME_MULT}")

# COMMAND ----------

# Ensure monitoring table exists (stable per club_id)
if not spark.catalog.tableExists(tbl(TABLE_MON)):
    clubs_df = spark.table(tbl("clubs")).select("club_id")

    # deterministic monitoring assignment per club_id (not per day)
    seed_mon = seed_for(TABLE_MON, run_date=RUN_DATE)  # seed value is deterministic, but we don't want day-dependence
    # Use a fixed seed for monitoring so it doesn't change with backfills
    seed_mon = (seed_mon // 1000000) * 1000000 + 20200101  # pin date component

    monitor_df = (clubs_df
        .withColumn("is_monitored", (F.rand(seed_mon) < F.lit(MONITOR_RATE)))
        .withColumn(
            "station_id",
            F.when(
                F.col("is_monitored"),
                F.concat(F.lit("ST-"), F.lpad(F.col("club_id").cast("string"), 8, "0"))
            )
        )
        .withColumn("installed_date", F.lit(RUN_DATE).cast("date"))
        .withColumn("decommissioned_date", F.lit(None).cast("date"))
        .withColumn("created_at", F.current_timestamp())
    )

    monitor_df.write.mode("overwrite").format("delta").saveAsTable(tbl(TABLE_MON))
    print(f"Created {tbl(TABLE_MON)} with monitoring rate ~{MONITOR_RATE}")
else:
    print(f"Found existing {tbl(TABLE_MON)}")

# COMMAND ----------

# Load dims
clubs = spark.table(tbl("clubs")).select("club_id", "address_id")
addr  = spark.table(tbl("addresses")).select(
    "address_id", "municipality_name", "county_name", "latitude", "longitude"
)
mon   = spark.table(tbl(TABLE_MON)).select("club_id", "is_monitored", "station_id")

monitored = (clubs
    .join(mon.where(F.col("is_monitored") == True), "club_id", "inner")
    .join(addr, "address_id", "left")
    .select("club_id", "station_id", "municipality_name", "county_name", "latitude", "longitude")
)

# COMMAND ----------

# Weather generator (seasonality + latitude effect)
month = RUN_DATE.month
doy = int(RUN_DATE.strftime("%j"))

def _base_temp_c(lat: float, rnd_: random.Random) -> float:
    # Rough Norway seasonal sinusoid: peak in July, trough in Jan.
    # Latitude effect: colder further north.
    seasonal = 10.0 * math.sin((doy - 200) / 365.0 * 2 * math.pi)  # ~ -10..+10
    lat_adj = -0.25 * max(0.0, (lat - 58.0))  # ~0 at 58N; -2.5C at 68N
    noise = rnd_.gauss(0, 2.0)
    return 6.0 + seasonal + lat_adj + noise

def _precip_mm(temp: float, rnd_: random.Random) -> float:
    # More precip in shoulder seasons; a few stormy days.
    p_rain = 0.35
    if month in (10, 11, 12, 1, 2, 3):
        p_rain = 0.45
    if month in (6, 7, 8):
        p_rain = 0.28
    if rnd_.random() > p_rain:
        return 0.0
    amt = max(0.0, rnd_.gammavariate(1.5, 4.0))  # mean ~6
    if rnd_.random() < 0.03:
        amt *= rnd_.uniform(2.0, 4.0)
    return float(min(amt, 80.0))

def _wind_mps(rnd_: random.Random) -> float:
    w = abs(rnd_.gauss(4.0, 2.5))
    if rnd_.random() < 0.02:
        w *= rnd_.uniform(2.0, 3.0)
    return float(min(w, 28.0))

def _condition(precip: float, snow: float) -> str:
    if precip == 0 and snow == 0:
        return "clear"
    if snow > 0:
        return "snow"
    if precip > 20:
        return "heavy_rain"
    return "rain"

@F.udf("struct<avg_temp_c:double,precip_mm:double,wind_mps:double,snow_cm:double,condition:string>")
def weather_udf(club_id, lat, lon):
    # Deterministic per (club_id, RUN_DATE)
    r = random.Random(seed_for(TABLE_DAILY, run_date=RUN_DATE) + int(club_id) * 13)
    lat_v = float(lat) if lat is not None else 60.0
    t = _base_temp_c(lat_v, r)
    p = _precip_mm(t, r)
    w = _wind_mps(r)
    snow = 0.0
    if t <= 1.0 and p > 0:
        snow = p * r.uniform(0.4, 1.2)  # mm water equivalent -> cm-ish proxy
    c = _condition(p, snow)
    return {"avg_temp_c": t, "precip_mm": p, "wind_mps": w, "snow_cm": snow, "condition": c}

df_out = (monitored
    .withColumn("wx", weather_udf("club_id", "latitude", "longitude"))
    .select(
        F.lit(RUN_DATE).cast("date").alias("weather_date"),
        "club_id",
        "station_id",
        F.col("wx.avg_temp_c").alias("avg_temp_c"),
        F.col("wx.precip_mm").alias("precip_mm"),
        F.col("wx.wind_mps").alias("wind_mps"),
        F.col("wx.snow_cm").alias("snow_cm"),
        F.col("wx.condition").alias("condition"),
        F.current_timestamp().alias("observed_at"),
        "municipality_name",
        "county_name"
    )
)

# COMMAND ----------

# DBTITLE 1,Cell 8
# Idempotent rerun: upsert on (weather_date, club_id)
df_weather_data = None  # Initialize at notebook level for Cell 9

if not spark.catalog.tableExists(tbl(TABLE_DAILY)):
    (df_out.write
        .mode("overwrite")
        .format("delta")
        .saveAsTable(tbl(TABLE_DAILY)))
    print(f"Created {tbl(TABLE_DAILY)} and inserted {df_out.count()} rows for {RUN_DATE.isoformat()}")
else:
    merge_into(TABLE_DAILY, df_out, ["weather_date", "club_id"])
    print(f"Upserted {df_out.count()} rows into {tbl(TABLE_DAILY)} for {RUN_DATE.isoformat()}")

df_weather_data = df_out  # Store at notebook level for Cell 9
display(df_out.limit(20))

# COMMAND ----------

# DBTITLE 1,Cell 9
# Export both weather tables to landing via the shared helper
# (overwrite per run_date partition — idempotent), so both reach bronze like the rest.
export_to_landing(TABLE_DAILY, df_weather_data)
# Monitoring config is slowly-changing — land the full table each run so bronze stays in sync.
export_to_landing(TABLE_MON, spark.table(tbl(TABLE_MON)))