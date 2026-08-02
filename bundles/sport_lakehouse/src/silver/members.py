from pyspark import pipelines as dp
from utils import prepare_for_cdc, deduplicate
from configs import MEMBERS as CONFIG

CATALOG = spark.conf.get("catalog")


@dp.temporary_view(name="members_prepared")
@dp.expect_or_fail("MEM_Member_Id not null", "MEM_Member_Id IS NOT NULL")
def members_prepared():
    df = spark.readStream.table(f"{CATALOG}.bronze.members")
    df = deduplicate(df, CONFIG)
    df = prepare_for_cdc(df, CONFIG)
    return df


dp.create_streaming_table(f"{CATALOG}.silver.members")

dp.create_auto_cdc_flow(
    target=f"{CATALOG}.silver.members",
    source="members_prepared",
    keys=["MEM_Rk"],
    sequence_by="_ingest_ts",
    stored_as_scd_type=2,
)
