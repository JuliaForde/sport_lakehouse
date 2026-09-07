from pyspark import pipelines as dp
from pyspark.sql import functions as F

# -----------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------
SOURCE_ROOT = spark.conf.get("landing.root")

def source_path(table: str) -> str:
    return f"{SOURCE_ROOT}/{table}/"


# -----------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------

def add_ingest_cols(df):
    """Add standard Bronze metadata columns.
    For historical files (landing date < today): simulate ingest at 10:30 on landing date.
    For today's files: use actual pipeline run time.
    """
    file_date = F.regexp_extract(F.col("_metadata.file_path"), r"/(\d{4}-\d{2}-\d{2})/", 1)
    # For historical files: deterministic random time between 10:30 and 11:37
    # hash(date) gives consistent jitter per date — same date always same time
    base_epoch = F.unix_timestamp(F.to_timestamp(file_date, "yyyy-MM-dd")) + (10 * 3600 + 30 * 60)
    jitter_secs = F.abs(F.hash(file_date)) % (67 * 60)
    ingest_ts = F.when(
        file_date.cast("date") < F.current_date(),
        F.to_timestamp(base_epoch + jitter_secs)
    ).otherwise(F.current_timestamp())
    return (
        df
        .withColumn("_ingest_ts",   ingest_ts)
        .withColumn("_ingest_date", F.to_date(ingest_ts))
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
@dp.expect("address_id not null", "address_id IS NOT NULL")
def addresses():
    return dedup_by_bk(add_ingest_cols(read_landing("addresses")), ["address_id"])


@dp.table(name="members", comment="Bronze: sport club members. PK: member_id.")
@dp.expect("member_id not null", "member_id IS NOT NULL")
def members():
    # Dedup by (member_id, _ingest_date) to keep one row per member per day.
    # Key-only dedup would drop same-day re-exports but also eat cross-day updates
    # (name changes, address moves), breaking SCD2 in silver.
    return dedup_by_bk(add_ingest_cols(read_landing("members")), ["member_id", "_ingest_date"])


@dp.table(name="clubs", comment="Bronze: sport clubs. PK: club_id.")
@dp.expect("club_id not null", "club_id IS NOT NULL")
def clubs():
    return dedup_by_bk(add_ingest_cols(read_landing("clubs")), ["club_id"])


@dp.table(name="sport_types", comment="Bronze: sport type reference data. PK: sport_type_id.")
@dp.expect("sport_type_id not null", "sport_type_id IS NOT NULL")
def sport_types():
    return dedup_by_bk(add_ingest_cols(read_landing("sport_types")), ["sport_type_id"])


@dp.table(name="club_sports", comment="Bronze: club-to-sport mappings. Composite PK: (club_id, sport_type_id).")
@dp.expect("club_id not null", "club_id IS NOT NULL")
@dp.expect("sport_type_id not null", "sport_type_id IS NOT NULL")
def club_sports():
    return dedup_by_bk(add_ingest_cols(read_landing("club_sports")), ["club_id", "sport_type_id"])


@dp.table(name="affiliations", comment="Bronze: member-club affiliations. PK: affiliation_id.")
@dp.expect("affiliation_id not null", "affiliation_id IS NOT NULL")
def affiliations():
    return dedup_by_bk(add_ingest_cols(read_landing("affiliations")), ["affiliation_id", "_ingest_date"])


@dp.table(name="memberships", comment="Bronze: membership subscriptions. PK: membership_id.")
@dp.expect("membership_id not null", "membership_id IS NOT NULL")
def memberships():
    return dedup_by_bk(add_ingest_cols(read_landing("memberships")), ["membership_id", "_ingest_date"])


@dp.table(name="membership_payments", comment="Bronze: membership payments. PK: payment_id.")
@dp.expect("payment_id not null", "payment_id IS NOT NULL")
def membership_payments():
    return dedup_by_bk(add_ingest_cols(read_landing("membership_payments")), ["payment_id", "_ingest_date"])


@dp.table(name="competitions", comment="Bronze: sport competitions. PK: competition_id.")
@dp.expect("competition_id not null", "competition_id IS NOT NULL")
def competitions():
    return dedup_by_bk(add_ingest_cols(read_landing("competitions")), ["competition_id"])


@dp.table(name="competition_waitlist", comment="Bronze: competition waitlist. PK: waitlist_id.")
@dp.expect("waitlist_id not null", "waitlist_id IS NOT NULL")
def competition_waitlist():
    return dedup_by_bk(add_ingest_cols(read_landing("competition_waitlist")), ["waitlist_id", "_ingest_date"])


@dp.table(name="participation", comment="Bronze: competition participation. PK: participation_id.")
@dp.expect("participation_id not null", "participation_id IS NOT NULL")
def participation():
    return dedup_by_bk(add_ingest_cols(read_landing("participation")), ["participation_id", "_ingest_date"])


@dp.table(name="results", comment="Bronze: competition results. PK: result_id.")
@dp.expect("result_id not null", "result_id IS NOT NULL")
def results():
    return dedup_by_bk(add_ingest_cols(read_landing("results")), ["result_id"])


# -----------------------------------------------------------------------
# Weather tables — seeded by bootstrap, continued by the daily notebooks
# (18_club_weather_daily / weather_stations_daily).
# -----------------------------------------------------------------------

@dp.table(name="club_weather_monitoring", comment="Bronze: which clubs are weather-monitored + their station. PK: club_id.")
@dp.expect("club_id not null", "club_id IS NOT NULL")
def club_weather_monitoring():
    return dedup_by_bk(add_ingest_cols(read_landing("club_weather_monitoring")), ["club_id"])


@dp.table(name="weather_stations", comment="Bronze: weather station dimension. PK: station_id.")
@dp.expect("station_id not null", "station_id IS NOT NULL")
def weather_stations():
    return dedup_by_bk(add_ingest_cols(read_landing("weather_stations")), ["station_id"])


@dp.table(name="club_weather_daily", comment="Bronze: daily weather observations per club. Composite PK: (weather_date, club_id).")
@dp.expect("weather_date not null", "weather_date IS NOT NULL")
@dp.expect("club_id not null", "club_id IS NOT NULL")
def club_weather_daily():
    return dedup_by_bk(add_ingest_cols(read_landing("club_weather_daily")), ["weather_date", "club_id"])
