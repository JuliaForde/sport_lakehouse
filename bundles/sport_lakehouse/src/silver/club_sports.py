from pyspark import pipelines as dp
from utils import prepare_for_cdc, deduplicate
from configs import CLUB_SPORTS as CONFIG

CATALOG = spark.conf.get("catalog")


@dp.temporary_view(name="club_sports_prepared")
@dp.expect_all_or_fail({
    "CLSP_Club_Id not null": "CLSP_Club_Id IS NOT NULL",
    "CLSP_SportType_Id not null": "CLSP_SportType_Id IS NOT NULL",
})
def club_sports_prepared():
    df = spark.readStream.table(f"{CATALOG}.bronze.club_sports")
    df = deduplicate(df, CONFIG)
    df = prepare_for_cdc(df, CONFIG)
    return df


dp.create_streaming_table(f"{CATALOG}.silver.club_sports")

dp.create_auto_cdc_flow(
    target=f"{CATALOG}.silver.club_sports",
    source="club_sports_prepared",
    keys=["CLSP_Rk"],
    sequence_by="_ingest_ts",
    stored_as_scd_type=2,
)
