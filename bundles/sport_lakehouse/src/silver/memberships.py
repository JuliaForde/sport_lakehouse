from pyspark import pipelines as dp
from utils import prepare_for_cdc, deduplicate
from configs import MEMBERSHIPS as CONFIG

CATALOG = spark.conf.get("catalog")


@dp.temporary_view(name="memberships_prepared")
@dp.expect_or_fail("MEMB_Membership_Id not null", "MEMB_Membership_Id IS NOT NULL")
def memberships_prepared():
    df = spark.readStream.table(f"{CATALOG}.bronze.memberships")
    df = deduplicate(df, CONFIG)
    df = prepare_for_cdc(df, CONFIG)
    return df


dp.create_streaming_table(f"{CATALOG}.silver.memberships")

dp.create_auto_cdc_flow(
    target=f"{CATALOG}.silver.memberships",
    source="memberships_prepared",
    keys=["MEMB_Rk"],
    sequence_by="_ingest_ts",
    stored_as_scd_type=2,
)
