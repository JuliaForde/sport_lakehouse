from pyspark import pipelines as dp
from utils import prepare_for_cdc, deduplicate, DEFAULT_HISTORY_EXCLUSIONS
from configs import PARTICIPATION as CONFIG

CATALOG = spark.conf.get("catalog")


@dp.temporary_view(name="participation_prepared")
@dp.expect_or_fail("PART_Participation_Id not null", "PART_Participation_Id IS NOT NULL")
def participation_prepared():
    df = spark.readStream.table(f"{CATALOG}.bronze.participation")
    df = deduplicate(df, CONFIG)
    df = prepare_for_cdc(df, CONFIG)
    return df


dp.create_streaming_table(f"{CATALOG}.silver.participation")

dp.create_auto_cdc_flow(
    target=f"{CATALOG}.silver.participation",
    source="participation_prepared",
    keys=["PART_Rk"],
    sequence_by="_ingest_ts",
    stored_as_scd_type=2,
    track_history_except_column_list=DEFAULT_HISTORY_EXCLUSIONS + ["PART_Rk"],
)
