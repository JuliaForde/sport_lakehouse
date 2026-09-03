from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import StringType

BRONZE_TECHNICAL_COLS = {"_ingest_ts", "_ingest_date", "_source_file"}

DEFAULT_HISTORY_EXCLUSIONS = ["_ingest_ts", "_ingest_date", "_source_file"]


def compute_rk(business_key_cols: list) -> F.Column:
    return F.xxhash64(*[F.col(c) for c in business_key_cols])


def compute_fk_rk(source_col: str) -> F.Column:
    """xxhash64 FK surrogate key with null guard — returns NULL if source col is NULL."""
    return (
        F.when(F.col(source_col).isNotNull(), F.xxhash64(F.col(source_col)))
        .otherwise(F.lit(None).cast("bigint"))
    )


def clean_strings(df: DataFrame) -> DataFrame:
    """Trim all string columns and coerce empty strings to NULL."""
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(
                field.name,
                F.when(F.trim(F.col(field.name)) == "", F.lit(None)).otherwise(F.trim(F.col(field.name)))
            )
    return df


def deduplicate(df: DataFrame, config: dict) -> DataFrame:
    return df.dropDuplicates(config["business_key"] + ["_ingest_ts"])


def prepare_for_cdc(df: DataFrame, config: dict) -> DataFrame:
    prefix         = config["prefix"]
    business_key   = config["business_key"]
    column_mapping = config["column_mapping"]

    df = clean_strings(df)

    for source_col, target_col in column_mapping.items():
        if source_col in df.columns:
            df = df.withColumnRenamed(source_col, target_col)

    renamed_bk_cols = [column_mapping.get(c, c) for c in business_key]
    df = df.withColumn(f"{prefix}_Rk", compute_rk(renamed_bk_cols))

    for fk in config.get("fk_rks", []):
        df = df.withColumn(fk["target_col"], compute_fk_rk(fk["source_col"]))

    cols_to_drop = [c for c in BRONZE_TECHNICAL_COLS - {"_ingest_ts"} if c in df.columns]
    if cols_to_drop:
        df = df.drop(*cols_to_drop)

    return df
