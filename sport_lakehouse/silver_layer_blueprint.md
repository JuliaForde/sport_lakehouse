# Silver Layer Blueprint

This document captures the architecture, rules, and reusable code for a Databricks silver layer
built with Spark Declarative Pipelines (SDP). Use it as a foundation when building a new silver
layer for a different domain — the pattern is domain-independent, only the table configs change.

---

## Core Rules

1. **Bronze is append-only** — never modify bronze. All transformations happen in silver.
2. **Every entity gets a surrogate key (RK)** — a stable integer hash of the business key, computed with `xxhash64`.
3. **History is tracked via SCD Type 2** — SDP manages `__START_AT` / `__END_AT` automatically via `stored_as_scd_type=2`.
4. **FK resolution is point-in-time** — when resolving a foreign key, join against the version of the referenced entity that was current at the time of the event, using the SCD2 time window.
5. **CDC key is the RK, not the business key** — the RK is what SDP uses to detect and track changes.
6. **Column naming follows a strict convention** — `PREFIX_AttributeName_DataType` (see below).

---

## Column Naming Convention

| Suffix | Meaning         | Example                    |
|--------|-----------------|----------------------------|
| `_Id`  | Source system ID | `MEM_Member_Id`            |
| `_Rk`  | Surrogate key    | `MEM_Rk`, `MEM_ADR_Rk`    |
| `_Name`| Name/label       | `MEM_FirstName_Name`       |
| `_Code`| Code/enum        | `MEM_Gender_Code`          |
| `_Desc`| Free text        | `MEM_Email_Desc`           |
| `_Dt`  | Date             | `MEM_BirthDate_Dt`         |
| `_Ts`  | Timestamp        | `MEM_CreatedAt_Ts`         |
| `_Flag`| Boolean          | `MEM_AcceptsMarketing_Flag`|
| `_No`  | Integer number   | `MEM_RelocationYear_No`    |
| `_Dec` | Decimal number   | `ADR_Latitude_Dec`         |

FK columns that reference another entity's RK are named `REFERENCING_PREFIX` + `_` + `REFERENCED_RK`:
e.g. `MEM_ADR_Rk` = the members table's reference to `ADR_Rk` (addresses surrogate key).

---

## What Bronze Must Provide

For this pattern to work, every bronze table needs:

| Column          | Purpose                                                              |
|-----------------|----------------------------------------------------------------------|
| `_ingest_ts`    | Timestamp of ingest — used for CDC sequencing and point-in-time FK joins |
| `_ingest_date`  | Date partition (for file tracking / incremental load)                |
| `_source_file`  | Source file path (audit trail)                                       |
| Business columns| All source columns, unmodified, as-is                                |

> If your bronze uses a different timestamp column name, update `sequence_by` in every
> `create_auto_cdc_flow` call and `event_time_col` in every FK lookup in configs.

---

## The 3-Step Table Pattern

Every silver table follows the same three steps in its notebook:

### Step 1 — Prepare a temporary view (streaming from bronze)

```python
@dp.temporary_view(name="<entity>_prepared")
def <entity>_prepared():
    df = spark.readStream.table("catalog.bronze.<entity>")

    # Apply any table-specific business logic here (filters, derived columns, etc.)

    df = deduplicate(df, CONFIG)        # drop duplicates within a micro-batch
    df = prepare_for_cdc(df, CONFIG)    # rename columns + compute RK

    for fk in CONFIG["fk_lookups"]:     # resolve FKs point-in-time (skip if no FKs)
        df = point_in_time_lookup(
            df=df,
            lookup_table=fk["lookup_table"],
            source_col=fk["source_col"],
            lookup_bk_col=fk["lookup_bk_col"],
            lookup_rk_col=fk["lookup_rk_col"],
            target_fk_col=fk["target_fk_col"],
            event_time_col=fk["event_time_col"],
            spark=spark,
        )

    return df
```

### Step 2 — Declare the silver streaming table

```python
dp.create_streaming_table("catalog.silver.<entity>")
```

### Step 3 — Wire up auto-CDC as SCD Type 2

```python
dp.create_auto_cdc_flow(
    target="catalog.silver.<entity>",
    source="<entity>_prepared",
    keys=["PREFIX_Rk"],         # always the RK, not the business key
    sequence_by="_ingest_ts",   # or whatever your bronze timestamp column is called
    stored_as_scd_type=2,
)
```

---

## Config Structure (one dict per table)

Define one config dict per entity. This is the only place domain knowledge lives.

```python
MY_ENTITY = {
    "prefix": "ENT",                        # short uppercase prefix for this entity
    "business_key": ["entity_id"],          # source column(s) that uniquely identify a row
                                            # use a list for composite keys: ["col_a", "col_b"]
    "deduplicate_by": "updated_at",         # optional: column to order by when deduplicating
                                            # omit (or set None) if source guarantees one row per key per batch
    "fk_lookups": [                         # list of FK references to other silver tables
        {
            "source_col":     "ENT_OtherEntity_Id",     # column in this table (after rename)
            "lookup_table":   "catalog.silver.other",   # silver table to join against
            "lookup_bk_col":  "OTH_OtherEntity_Id",     # business key col in the lookup table
            "lookup_rk_col":  "OTH_Rk",                 # RK col in the lookup table
            "target_fk_col":  "ENT_OTH_Rk",             # new column to add to this table
            "event_time_col": "_ingest_ts",              # timestamp for point-in-time join
        }
    ],
    "column_mapping": {
        # source_column_name: "PREFIX_TargetName_DataTypeSuffix"
        "entity_id":    "ENT_Entity_Id",
        "name":         "ENT_Entity_Name",
        "status":       "ENT_Status_Code",
        "created_at":   "ENT_CreatedAt_Ts",
    },
}
```

For a root table with no FKs:

```python
MY_ROOT = {
    "prefix": "ROOT",
    "business_key": ["root_id"],
    "fk_lookups": [],
    "column_mapping": {
        "root_id":   "ROOT_Root_Id",
        "name":      "ROOT_Name_Name",
    },
}
```

---

## Reusable Utility Functions (`utils.py`)

Copy this file verbatim into any new silver layer project. No domain-specific logic.

```python
from pyspark.sql import DataFrame, functions as F, Column, Window

# Technical columns added by Bronze — excluded from Silver business logic
BRONZE_TECHNICAL_COLS = {"_ingest_ts", "_ingest_date", "_source_file"}


def compute_rk(business_key_cols: list) -> Column:
    """Stable integer identity per business entity. Works for single and composite keys."""
    return F.xxhash64(*[F.col(c) for c in business_key_cols])


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
```

---

## Complete Example: Root Table (no FKs)

`src/pipeline/addresses.py`:

```python
from pyspark import pipelines as dp
from utils import prepare_for_cdc, deduplicate
from configs import ADDRESSES as CONFIG


@dp.temporary_view(name="addresses_prepared")
def addresses_prepared():
    df = spark.readStream.table("sport_lakehouse.bronze.addresses")
    df = deduplicate(df, CONFIG)
    df = prepare_for_cdc(df, CONFIG)
    return df


dp.create_streaming_table("sport_lakehouse.silver.addresses")

dp.create_auto_cdc_flow(
    target="sport_lakehouse.silver.addresses",
    source="addresses_prepared",
    keys=["ADR_Rk"],
    sequence_by="_ingest_ts",
    stored_as_scd_type=2,
)
```

## Complete Example: Table with FK (depends on addresses)

`src/pipeline/members.py`:

```python
from pyspark import pipelines as dp
from utils import prepare_for_cdc, deduplicate, point_in_time_lookup
from configs import MEMBERS as CONFIG


@dp.temporary_view(name="members_prepared")
def members_prepared():
    df = spark.readStream.table("sport_lakehouse.bronze.members")

    df = deduplicate(df, CONFIG)
    df = prepare_for_cdc(df, CONFIG)

    for fk in CONFIG["fk_lookups"]:
        df = point_in_time_lookup(
            df=df,
            lookup_table=fk["lookup_table"],
            source_col=fk["source_col"],
            lookup_bk_col=fk["lookup_bk_col"],
            lookup_rk_col=fk["lookup_rk_col"],
            target_fk_col=fk["target_fk_col"],
            event_time_col=fk["event_time_col"],
            spark=spark,
        )

    return df


dp.create_streaming_table("sport_lakehouse.silver.members")

dp.create_auto_cdc_flow(
    target="sport_lakehouse.silver.members",
    source="members_prepared",
    keys=["MEM_Rk"],
    sequence_by="_ingest_ts",
    stored_as_scd_type=2,
)
```

---

## What to Reuse vs. Rebuild

| Part | Action |
|------|--------|
| `utils.py` | Copy verbatim — no domain logic |
| 3-step notebook pattern | Use as template for every new table |
| `configs.py` | Rebuild from scratch for your domain/tables |
| Individual table notebooks | Rebuild using the template above |
| Pipeline YAML | Rebuild — list your notebooks in dependency order |

---

## Dependency Ordering Rule

Tables must be declared in the pipeline in dependency order — a table that has an FK to another
must be declared after it. Root tables (no FKs) go first.

```
root tables (no FKs)
    ↓
tables that reference root tables
    ↓
tables that reference the above
    ↓
...and so on
```
