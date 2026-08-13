# Databricks notebook source
from pathlib import Path


METRIC_VIEWS_DIR = Path.cwd()
DDL_FILES = sorted(METRIC_VIEWS_DIR.glob("*.sql"))

if not DDL_FILES:
    raise FileNotFoundError(f"No metric view DDL files found in {METRIC_VIEWS_DIR}")

spark.sql("CREATE SCHEMA IF NOT EXISTS sport_lakehouse.gold")

for ddl_file in DDL_FILES:
    print(f"Deploying metric view from {ddl_file.name}")
    spark.sql(ddl_file.read_text(encoding="utf-8"))

print(f"Deployed {len(DDL_FILES)} metric view(s)")
