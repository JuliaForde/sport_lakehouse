from pyspark import pipelines as dp

CATALOG = spark.conf.get("catalog")


@dp.materialized_view(name=f"{CATALOG}.gold.membership_payments")
def membership_payments():
    return spark.sql(f"""
        SELECT
            MEPA_Rk,
            MEPA_MEM_Rk,
            MEPA_MEMB_Rk,
            MEPA_Payment_Id         AS payment_id,
            MEPA_Member_Id          AS member_id,
            MEPA_Membership_Id      AS membership_id,
            MEPA_AmountNok_Dec      AS amount_nok,
            MEPA_Currency_Code      AS currency,
            MEPA_Method_Code        AS method,
            MEPA_Status_Code        AS status,
            MEPA_Attempt_No         AS attempt,
            MEPA_CreatedAt_Ts       AS created_at,
            MEPA_PaidAt_Ts          AS paid_at,
            MEPA_UpdatedAt_Ts       AS updated_at
        FROM {CATALOG}.silver.membership_payments
        WHERE __END_AT IS NULL
    """)
