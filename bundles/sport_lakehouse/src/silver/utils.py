from pyspark.sql import DataFrame, functions as F

BRONZE_TECHNICAL_COLS = {"_ingest_ts", "_ingest_date", "_source_file"}


def compute_rk(business_key_cols: list) -> F.Column:
    return F.xxhash64(*[F.col(c) for c in business_key_cols])


def deduplicate(df: DataFrame, config: dict) -> DataFrame:
    return df.dropDuplicates(config["business_key"])


def prepare_for_cdc(df: DataFrame, config: dict) -> DataFrame:
    prefix         = config["prefix"]
    business_key   = config["business_key"]
    column_mapping = config["column_mapping"]

    for source_col, target_col in column_mapping.items():
        if source_col in df.columns:
            df = df.withColumnRenamed(source_col, target_col)

    renamed_bk_cols = [column_mapping.get(c, c) for c in business_key]
    df = df.withColumn(f"{prefix}_Rk", compute_rk(renamed_bk_cols))

    for fk in config.get("fk_rks", []):
        df = df.withColumn(fk["target_col"], compute_rk([fk["source_col"]]))

    cols_to_drop = [c for c in BRONZE_TECHNICAL_COLS - {"_ingest_ts"} if c in df.columns]
    if cols_to_drop:
        df = df.drop(*cols_to_drop)

    return df
