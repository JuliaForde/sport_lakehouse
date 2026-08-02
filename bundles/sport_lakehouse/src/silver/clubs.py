from pyspark import pipelines as dp
from utils import prepare_for_cdc, deduplicate
from configs import CLUBS as CONFIG

CATALOG = spark.conf.get("catalog")


@dp.temporary_view(name="clubs_prepared")
@dp.expect_or_fail("CLB_Club_Id not null", "CLB_Club_Id IS NOT NULL")
def clubs_prepared():
    df = spark.readStream.table(f"{CATALOG}.bronze.clubs")
    df = deduplicate(df, CONFIG)
    df = prepare_for_cdc(df, CONFIG)
    return df


dp.create_streaming_table(f"{CATALOG}.silver.clubs")

dp.create_auto_cdc_flow(
    target=f"{CATALOG}.silver.clubs",
    source="clubs_prepared",
    keys=["CLB_Rk"],
    sequence_by="_ingest_ts",
    stored_as_scd_type=2,
)
