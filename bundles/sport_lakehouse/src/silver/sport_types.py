from pyspark import pipelines as dp
from utils import prepare_for_cdc, deduplicate
from configs import SPORT_TYPES as CONFIG

CATALOG = spark.conf.get("catalog")


@dp.temporary_view(name="sport_types_prepared")
@dp.expect_or_fail("SPTP_SportType_Id not null", "SPTP_SportType_Id IS NOT NULL")
def sport_types_prepared():
    df = spark.readStream.table(f"{CATALOG}.bronze.sport_types")
    df = deduplicate(df, CONFIG)
    df = prepare_for_cdc(df, CONFIG)
    return df


dp.create_streaming_table(f"{CATALOG}.silver.sport_types")

dp.create_auto_cdc_flow(
    target=f"{CATALOG}.silver.sport_types",
    source="sport_types_prepared",
    keys=["SPTP_Rk"],
    sequence_by="_ingest_ts",
    stored_as_scd_type=2,
)
