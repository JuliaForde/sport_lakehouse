# Databricks notebook source
# 00_utils — Common utilities for Norwegian sports Lakehouse simulation (Delta + UC friendly)
# Target: demo_data.sports (defaults), with run_date + volume controls.

from pyspark.sql import functions as F
from datetime import date, timedelta, datetime
import random

# COMMAND ----------
# ---- Target catalog/schema (safe defaults; can be overridden by Spark conf if present) ----
def _safe_conf_get(key: str, default: str) -> str:
    try:
        v = spark.conf.get(key)
        return v if v else default
    except Exception:
        return default

TARGET_CATALOG = _safe_conf_get("sports.target_catalog", "demo_data")
TARGET_SCHEMA  = _safe_conf_get("sports.target_schema",  "sports")

# Set them back into conf so downstream code can safely read them even if conf was missing
try:
    spark.conf.set("sports.target_catalog", TARGET_CATALOG)
    spark.conf.set("sports.target_schema",  TARGET_SCHEMA)
except Exception:
    pass

def tbl(name: str) -> str:
    return f"`{TARGET_CATALOG}`.`{TARGET_SCHEMA}`.`{name}`"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{TARGET_CATALOG}`.`{TARGET_SCHEMA}`")

# COMMAND ----------
# ---- Parameters ----
# In a Databricks Job, pass widgets:
#   run_date=YYYY-MM-DD
#   volume=low|medium|high
#   volume_factor=1.0 (optional, multiplies volume multiplier)

dbutils.widgets.text("run_date", "")
dbutils.widgets.text("volume", "medium")
dbutils.widgets.text("volume_factor", "1.0")

RUN_DATE_STR = (dbutils.widgets.get("run_date") or "").strip()
RUN_DATE = date.fromisoformat(RUN_DATE_STR) if RUN_DATE_STR else date.today()

VOLUME = (dbutils.widgets.get("volume") or "medium").strip().lower()
try:
    VOLUME_FACTOR = float((dbutils.widgets.get("volume_factor") or "1.0").strip())
except Exception:
    VOLUME_FACTOR = 1.0

BASE_MULT = {"low": 0.5, "medium": 1.0, "high": 2.0}.get(VOLUME, 1.0)
VOLUME_MULT = max(0.1, BASE_MULT * VOLUME_FACTOR)

print(f"RUN_DATE={RUN_DATE.isoformat()}  VOLUME={VOLUME}  VOLUME_MULT={VOLUME_MULT}")

# COMMAND ----------
# ---- Deterministic randomness per day+table ----
def seed_for(table_name: str, run_date: date = RUN_DATE, base_seed: int = 42) -> int:
    ymd = int(run_date.strftime("%Y%m%d"))
    return (base_seed * 1_000_000) + ymd + (abs(hash(table_name)) % 10_000)

# COMMAND ----------
# ---- State table to make daily notebooks re-runnable/idempotent ----
# We store the allocated ID ranges per (table_name, run_date)
META_TABLE = tbl("sim_state")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {META_TABLE} (
  table_name STRING,
  run_date DATE,
  start_id BIGINT,
  end_id BIGINT,
  n_rows BIGINT,
  notes STRING,
  updated_at TIMESTAMP
)
USING DELTA
""")

# ---- Internal state key ----
# `owner` lets two notebooks allocate the SAME table on the SAME run_date
# without colliding (e.g. 06_addresses_daily and clubs_daily both create
# addresses). Each owner gets its own sim_state row, but ID ranges are still
# based on the real table max id so they never overlap when run sequentially.
def _state_key(table_name: str, owner: str = None) -> str:
    return table_name if not owner else f"{table_name}::{owner}"

def _state_for_date(state_key: str, run_date: date = RUN_DATE):
    df = (spark.table(META_TABLE)
            .where(F.col("table_name") == state_key)
            .where(F.col("run_date") == F.lit(run_date)))
    row = df.orderBy(F.col("updated_at").desc()).limit(1).collect()
    return row[0].asDict() if row else None

def _prev_end_id(state_key: str, run_date: date = RUN_DATE) -> int:
    df = (spark.table(META_TABLE)
            .where(F.col("table_name") == state_key)
            .where(F.col("run_date") < F.lit(run_date)))
    r = df.agg(F.max("end_id").alias("mx")).collect()[0]["mx"]
    return int(r or 0)

def table_max_id(table_name: str, id_col: str) -> int:
    try:
        r = spark.table(tbl(table_name)).agg(F.max(F.col(id_col)).alias("mx")).collect()[0]["mx"]
        return int(r or 0)
    except Exception:
        return 0

def allocate_ids(table_name: str, id_col: str, proposed_n_rows: int, owner: str = None):
    """Allocate a stable ID range for this run_date.
    - If already allocated today (for this owner), returns the same range (idempotent rerun).
    - Else starts at max(previous end_id for this owner, current REAL table max id).
      Basing the floor on the real table max guarantees no overlap with ranges
      another owner allocated on the same table/day, as long as runs are sequential.
    """
    state_key = _state_key(table_name, owner)
    existing = _state_for_date(state_key, RUN_DATE)
    if existing:
        return {
            "already_ran": True,
            "start_id": int(existing["start_id"]),
            "end_id": int(existing["end_id"]),
            "n_rows": int(existing["n_rows"]),
        }

    start_id = max(_prev_end_id(state_key, RUN_DATE), table_max_id(table_name, id_col))
    n_rows = int(max(0, proposed_n_rows))
    end_id = start_id + n_rows

    return {"already_ran": False, "start_id": start_id, "end_id": end_id, "n_rows": n_rows}

def record_state(table_name: str, start_id: int, end_id: int, n_rows: int, notes: str = "", owner: str = None):
    spark.createDataFrame([{
        "table_name": _state_key(table_name, owner),
        "run_date": RUN_DATE,
        "start_id": int(start_id),
        "end_id": int(end_id),
        "n_rows": int(n_rows),
        "notes": notes,
        "updated_at": datetime.utcnow()
    }]).write.mode("append").saveAsTable(META_TABLE)

# COMMAND ----------
# ---- Merge helper ----
def merge_into(name: str, df, key_cols: list[str]):
    temp = f"tmp_{name}_{int(datetime.utcnow().timestamp())}"
    df.createOrReplaceTempView(temp)
    on = " AND ".join([f"t.{c} = s.{c}" for c in key_cols])
    spark.sql(f"""
      MERGE INTO {tbl(name)} t
      USING {temp} s
      ON {on}
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *
    """)

# COMMAND ----------
# ---- Landing export helper (shared by ALL daily notebooks) ----
# Single source of truth for how a daily batch reaches the bronze Auto Loader.
# Always OVERWRITE the (table, run_date) partition:
#   - idempotent: re-running the same day replaces that day's drop (no duplicate files)
#   - safe: the path is date-partitioned, so previous days and the bootstrap drop
#     are never touched.
LANDING_ROOT = "/Volumes/sport_lakehouse/landing/sports"

def export_to_landing(name: str, df, run_date: date = RUN_DATE, owner: str = None):
    """Write a daily batch to the landing volume as parquet for bronze ingestion.

    `owner` adds a subfolder (e.g. .../addresses/<date>/clubs/) so two notebooks
    can write to the SAME table on the SAME day without clobbering each other.
    Auto Loader reads the table path recursively, so subfolders are picked up.
    """
    if df is None:
        print(f"⏭️  {name}: nothing to export (no new rows)")
        return
    sub = f"{owner}/" if owner else ""
    path = f"{LANDING_ROOT}/{name}/{run_date}/{sub}"
    df.write.mode("overwrite").parquet(path)
    print(f"✅ {name}: exported to {path}")
