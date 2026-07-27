from pyspark.sql import DataFrame, functions as F, Column, Window

# -----------------------------------------------------------------------
# Technical columns added by Bronze — excluded from Silver business logic
# -----------------------------------------------------------------------
BRONZE_TECHNICAL_COLS = {"_ingest_ts", "_ingest_date", "_source_file"}


# -----------------------------------------------------------------------
# Hashing
# -----------------------------------------------------------------------

def compute_rk(business_key_cols: list) -> Column:
    """Stable integer identity per business entity. Works for single and composite keys."""
    return F.xxhash64(*[F.col(c) for c in business_key_cols])


# -----------------------------------------------------------------------
# Deduplication
# -----------------------------------------------------------------------

def deduplicate(df: DataFrame, config: dict) -> DataFrame:
    """
    Keeps only the latest row per business key within a batch.
    Only active when 'deduplicate_by' is set in config.
    If None — source guarantees one row per business key per file, skip dedup.
    """
    order_col = config.get("deduplicate_by")
    if not order_col:
        return df

    business_key = config["business_key"]
    window = Window.partitionBy(*business_key).orderBy(F.desc(order_col))

    return (
        df.withColumn("_row_num", F.row_number().over(window))
          .filter(F.col("_row_num") == 1)
          .drop("_row_num")
    )


# -----------------------------------------------------------------------
# CDC preparation
# -----------------------------------------------------------------------

def prepare_for_cdc(df: DataFrame, config: dict) -> DataFrame:
    """
    Generic preparation before AUTO CDC:
    - Rename columns using prefix/suffix mapping from config
    - Compute RK (stable entity identity via xxhash64)

    AUTO CDC handles change detection automatically — no hash needed.
    Business logic specific to a table should be applied BEFORE calling this.
    """
    prefix         = config["prefix"]
    business_key   = config["business_key"]
    column_mapping = config["column_mapping"]

    for source_col, target_col in column_mapping.items():
        if source_col in df.columns:
            df = df.withColumnRenamed(source_col, target_col)

    renamed_bk_cols = [column_mapping.get(c, c) for c in business_key]

    df = df.withColumn(f"{prefix}_Rk", compute_rk(renamed_bk_cols))

    return df


# -----------------------------------------------------------------------
# FK lookup (point-in-time)
# -----------------------------------------------------------------------

def point_in_time_lookup(
    df: DataFrame,
    lookup_table: str,
    source_col: str,
    lookup_bk_col: str,
    lookup_rk_col: str,
    target_fk_col: str,
    event_time_col: str,
    spark,
) -> DataFrame:
    """
    Resolves a business key to an RK via point-in-time join against a Silver SCD2 table.

    Joins on:
        source_col == lookup_bk_col
        AND event_time >= __START_AT
        AND (event_time < __END_AT OR __END_AT IS NULL)
    """
    lookup_df = spark.read.table(lookup_table).select(
        lookup_bk_col, lookup_rk_col, "__START_AT", "__END_AT"
    )

    df = (
        df.join(
            lookup_df,
            (df[source_col] == lookup_df[lookup_bk_col])
            & (df[event_time_col] >= lookup_df["__START_AT"])
            & (lookup_df["__END_AT"].isNull() | (df[event_time_col] < lookup_df["__END_AT"])),
            "left",
        )
        .withColumn(target_fk_col, F.col(lookup_rk_col))
        .drop(lookup_bk_col, lookup_rk_col, "__START_AT", "__END_AT")
    )

    return df
