from pyspark import pipelines as dp
from utils import prepare_for_cdc, deduplicate, DEFAULT_HISTORY_EXCLUSIONS
from configs import COMPETITION_WAITLIST as CONFIG

CATALOG = spark.conf.get("catalog")


@dp.temporary_view(name="competition_waitlist_prepared")
@dp.expect_or_fail("COWL_Waitlist_Id not null", "COWL_Waitlist_Id IS NOT NULL")
def competition_waitlist_prepared():
    df = spark.readStream.table(f"{CATALOG}.bronze.competition_waitlist")
    df = deduplicate(df, CONFIG)
    df = prepare_for_cdc(df, CONFIG)
    return df


dp.create_streaming_table(f"{CATALOG}.silver.competition_waitlist")

dp.create_auto_cdc_flow(
    target=f"{CATALOG}.silver.competition_waitlist",
    source="competition_waitlist_prepared",
    keys=["COWL_Rk"],
    sequence_by="_ingest_ts",
    stored_as_scd_type=2,
    track_history_except_column_list=DEFAULT_HISTORY_EXCLUSIONS + ["COWL_Rk"],
)
