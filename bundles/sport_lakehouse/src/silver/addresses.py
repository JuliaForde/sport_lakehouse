from pyspark import pipelines as dp
from utils import prepare_for_cdc, deduplicate, DEFAULT_HISTORY_EXCLUSIONS
from configs import ADDRESSES as CONFIG

CATALOG = spark.conf.get("catalog")


@dp.temporary_view(name="addresses_prepared")
@dp.expect_or_fail("ADR_Address_Id not null", "ADR_Address_Id IS NOT NULL")
def addresses_prepared():
    df = spark.readStream.table(f"{CATALOG}.bronze.addresses")
    df = deduplicate(df, CONFIG)
    df = prepare_for_cdc(df, CONFIG)
    return df


dp.create_streaming_table(f"{CATALOG}.silver.addresses")

dp.create_auto_cdc_flow(
    target=f"{CATALOG}.silver.addresses",
    source="addresses_prepared",
    keys=["ADR_Rk"],
    sequence_by="_ingest_ts",
    stored_as_scd_type=2,
    track_history_except_column_list=DEFAULT_HISTORY_EXCLUSIONS + ["ADR_Rk"],
)
