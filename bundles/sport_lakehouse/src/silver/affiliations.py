from pyspark import pipelines as dp
from utils import prepare_for_cdc, deduplicate
from configs import AFFILIATIONS as CONFIG

CATALOG = spark.conf.get("catalog")


@dp.temporary_view(name="affiliations_prepared")
@dp.expect_or_fail("AFF_Affiliation_Id not null", "AFF_Affiliation_Id IS NOT NULL")
def affiliations_prepared():
    df = spark.readStream.table(f"{CATALOG}.bronze.affiliations")
    df = deduplicate(df, CONFIG)
    df = prepare_for_cdc(df, CONFIG)
    return df


dp.create_streaming_table(f"{CATALOG}.silver.affiliations")

dp.create_auto_cdc_flow(
    target=f"{CATALOG}.silver.affiliations",
    source="affiliations_prepared",
    keys=["AFF_Rk"],
    sequence_by="_ingest_ts",
    stored_as_scd_type=2,
)
