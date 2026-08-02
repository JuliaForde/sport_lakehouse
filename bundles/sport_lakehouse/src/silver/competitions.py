from pyspark import pipelines as dp
from utils import prepare_for_cdc, deduplicate
from configs import COMPETITIONS as CONFIG

CATALOG = spark.conf.get("catalog")


@dp.temporary_view(name="competitions_prepared")
@dp.expect_or_fail("COMP_Competition_Id not null", "COMP_Competition_Id IS NOT NULL")
def competitions_prepared():
    df = spark.readStream.table(f"{CATALOG}.bronze.competitions")
    df = deduplicate(df, CONFIG)
    df = prepare_for_cdc(df, CONFIG)
    return df


dp.create_streaming_table(f"{CATALOG}.silver.competitions")

dp.create_auto_cdc_flow(
    target=f"{CATALOG}.silver.competitions",
    source="competitions_prepared",
    keys=["COMP_Rk"],
    sequence_by="_ingest_ts",
    stored_as_scd_type=2,
)
