from pyspark import pipelines as dp
from utils import prepare_for_cdc, deduplicate
from configs import MEMBERSHIP_PAYMENTS as CONFIG

CATALOG = spark.conf.get("catalog")


@dp.temporary_view(name="membership_payments_prepared")
@dp.expect_or_fail("MEPA_Payment_Id not null", "MEPA_Payment_Id IS NOT NULL")
def membership_payments_prepared():
    df = spark.readStream.table(f"{CATALOG}.bronze.membership_payments")
    df = deduplicate(df, CONFIG)
    df = prepare_for_cdc(df, CONFIG)
    return df


dp.create_streaming_table(f"{CATALOG}.silver.membership_payments")

dp.create_auto_cdc_flow(
    target=f"{CATALOG}.silver.membership_payments",
    source="membership_payments_prepared",
    keys=["MEPA_Rk"],
    sequence_by="_ingest_ts",
    stored_as_scd_type=2,
)
