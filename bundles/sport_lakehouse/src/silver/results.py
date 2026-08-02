from pyspark import pipelines as dp
from utils import prepare_for_cdc, deduplicate
from configs import RESULTS as CONFIG

CATALOG = spark.conf.get("catalog")


@dp.temporary_view(name="results_prepared")
@dp.expect_or_fail("RES_Result_Id not null", "RES_Result_Id IS NOT NULL")
def results_prepared():
    df = spark.readStream.table(f"{CATALOG}.bronze.results")
    df = deduplicate(df, CONFIG)
    df = prepare_for_cdc(df, CONFIG)
    return df


dp.create_streaming_table(f"{CATALOG}.silver.results")

dp.create_auto_cdc_flow(
    target=f"{CATALOG}.silver.results",
    source="results_prepared",
    keys=["RES_Rk"],
    sequence_by="_ingest_ts",
    stored_as_scd_type=2,
)
