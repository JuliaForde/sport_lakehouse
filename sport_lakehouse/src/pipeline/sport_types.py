from pyspark import pipelines as dp
from utils import prepare_for_cdc, deduplicate
from configs import SPORT_TYPES as CONFIG


@dp.temporary_view(name="sport_types_prepared")
def sport_types_prepared():
    df = spark.readStream.table("sport_lakehouse.bronze.sport_types")
    df = deduplicate(df, CONFIG)
    df = prepare_for_cdc(df, CONFIG)
    return df


dp.create_streaming_table("sport_lakehouse.silver.sport_types")

dp.create_auto_cdc_flow(
    target="sport_lakehouse.silver.sport_types",
    source="sport_types_prepared",
    keys=["SPTP_Rk"],
    sequence_by="_ingest_ts",
    stored_as_scd_type=2,
)
