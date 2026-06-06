from pyspark import pipelines as dp
from pyspark.sql import functions as F

# -----------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------
SOURCE_ROOT = "/Volumes/sport_lakehouse/landing/sports"

def source_path(table: str) -> str:
    return f"{SOURCE_ROOT}/{table}/"


# -----------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------

def add_ingest_cols(df):
    """Add standard Bronze metadata columns."""
    return (
        df
        .withColumn("_ingest_ts",   F.current_timestamp())
        .withColumn("_ingest_date", F.to_date(F.current_timestamp()))
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )


def dedup_by_bk(df, bk_cols: list):
    """
    Deduplicate per business key within each streaming micro-batch.
    Uses dropDuplicates() which is supported in Structured Streaming.
    Silver AUTO CDC handles change detection and versioning.
    """
    return df.dropDuplicates(bk_cols)


def read_landing(table: str):
    """Read parquet files from landing volume using Auto Loader.
    Schema is inferred from Parquet files (self-describing format).
    """
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(source_path(table))
    )


# -----------------------------------------------------------------------
# Bronze tables
# -----------------------------------------------------------------------

@dp.table(name="addresses", comment="Bronze: address reference data. PK: address_id.")
def addresses():
    return dedup_by_bk(add_ingest_cols(read_landing("addresses")), ["address_id"])


@dp.table(name="members", comment="Bronze: sport club members. PK: member_id.")
def members():
    return dedup_by_bk(add_ingest_cols(read_landing("members")), ["member_id"])


@dp.table(name="clubs", comment="Bronze: sport clubs. PK: club_id.")
def clubs():
    return dedup_by_bk(add_ingest_cols(read_landing("clubs")), ["club_id"])


@dp.table(name="sport_types", comment="Bronze: sport type reference data. PK: sport_type_id.")
def sport_types():
    return dedup_by_bk(add_ingest_cols(read_landing("sport_types")), ["sport_type_id"])


@dp.table(name="club_sports", comment="Bronze: club-to-sport mappings. Composite PK: (club_id, sport_type_id).")
def club_sports():
    return dedup_by_bk(add_ingest_cols(read_landing("club_sports")), ["club_id", "sport_type_id"])


@dp.table(name="affiliations", comment="Bronze: member-club affiliations. PK: affiliation_id.")
def affiliations():
    return dedup_by_bk(add_ingest_cols(read_landing("affiliations")), ["affiliation_id"])


@dp.table(name="memberships", comment="Bronze: membership subscriptions. PK: membership_id.")
def memberships():
    return dedup_by_bk(add_ingest_cols(read_landing("memberships")), ["membership_id"])


@dp.table(name="membership_payments", comment="Bronze: membership payments. PK: payment_id.")
def membership_payments():
    return dedup_by_bk(add_ingest_cols(read_landing("membership_payments")), ["payment_id"])


@dp.table(name="competitions", comment="Bronze: sport competitions. PK: competition_id.")
def competitions():
    return dedup_by_bk(add_ingest_cols(read_landing("competitions")), ["competition_id"])


@dp.table(name="competition_waitlist", comment="Bronze: competition waitlist. PK: waitlist_id.")
def competition_waitlist():
    return dedup_by_bk(add_ingest_cols(read_landing("competition_waitlist")), ["waitlist_id"])


@dp.table(name="participation", comment="Bronze: competition participation. PK: participation_id.")
def participation():
    return dedup_by_bk(add_ingest_cols(read_landing("participation")), ["participation_id"])


@dp.table(name="results", comment="Bronze: competition results. PK: result_id.")
def results():
    return dedup_by_bk(add_ingest_cols(read_landing("results")), ["result_id"])


# -----------------------------------------------------------------------
# Desse aktiveres etter at daily notebooks er køyrt:
# - club_weather_daily        → 18_club_weather_daily
# - club_weather_monitoring   → 18_club_weather_daily
# - weather_stations          → weather_stations_daily
# -----------------------------------------------------------------------
