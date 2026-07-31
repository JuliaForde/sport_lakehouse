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
