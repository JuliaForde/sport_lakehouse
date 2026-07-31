# Databricks notebook source
# Databricks notebook source
# 09_weather_stations_daily — Slowly growing stations dimension derived from club_weather_monitoring
# - Uses widgets from 00_utils: run_date / volume / volume_factor (exposed as RUN_DATE, VOLUME, VOLUME_MULT)
# - Derives new stations from club_weather_monitoring
# - Writes via merge_into() to avoid duplicates on reruns
#
# Prereq: Run 00_bootstrap_generate_sports_dataset first to create the base tables.

# COMMAND ----------

# COMMAND ----------
# Create/override the widgets that 00_utils actually reads
from datetime import date

dbutils.widgets.text("run_date", date.today().isoformat())
dbutils.widgets.text("volume", "medium")  # low|medium|high
dbutils.widgets.text("volume_factor", "1.0")  # scales volume_mult

# COMMAND ----------

# MAGIC %run ./00_utils

# COMMAND ----------

# DBTITLE 1,Cell 4

from pyspark.sql import functions as F

TABLE = "weather_stations"
SOURCE_MON = "club_weather_monitoring"
df_new_stations = None  # Initialize at notebook level for export

print(f"RUN_DATE={RUN_DATE.isoformat()}  VOLUME={VOLUME}  VOLUME_MULT={VOLUME_MULT}")

# COMMAND ----------
# Ensure target table exists (minimal dimension schema)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {tbl(TABLE)} (
  station_id STRING,
  club_id BIGINT,
  installed_date DATE,
  decommissioned_date DATE,
  is_active BOOLEAN
)
USING DELTA
""")

# COMMAND ----------
# Read monitoring assignments (stable per club)
mon = spark.table(tbl(SOURCE_MON))

# Keep only monitored rows that have station_id
monitored = (mon
    .where(F.col("is_monitored") == F.lit(True))
    .where(F.col("station_id").isNotNull())
    .select(
        F.col("station_id").cast("string"),
        F.col("club_id").cast("bigint"),
        F.col("installed_date").cast("date"),
        F.col("decommissioned_date").cast("date"),
    )
)

# Derive "station dimension" rows from monitoring
stations_derived = (monitored
    .withColumn("is_active", F.col("decommissioned_date").isNull())
)

# COMMAND ----------
# Only take stations that are not already in the stations dimension
existing = spark.table(tbl(TABLE)).select("station_id").distinct()

df_new = (stations_derived
    .join(existing, on="station_id", how="left_anti")
    .dropDuplicates(["station_id"])
)

n_new = df_new.count()
print(f"New stations today: {n_new}")

if n_new == 0:
    dbutils.notebook.exit("No new weather stations today.")

# Cache the DataFrame BEFORE merge to preserve for export
df_new_stations = df_new
_ = df_new_stations.count()  # Force materialization

# COMMAND ----------
# Merge into Delta dimension table
merge_into(TABLE, df_new_stations, ["station_id"])
print(f"✅ Merged {n_new} stations into weather_stations table")


# COMMAND ----------

# DBTITLE 1,Cell 5
# Export weather stations via the shared helper (overwrite per run_date partition — idempotent).
export_to_landing(TABLE, df_new_stations)

print("✅ weather_stations_daily complete")