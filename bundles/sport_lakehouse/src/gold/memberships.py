from pyspark import pipelines as dp

CATALOG = spark.conf.get("catalog")


@dp.materialized_view(name=f"{CATALOG}.gold.memberships")
def memberships():
    return spark.sql(f"""
        SELECT
            MEMB_Rk,
            MEMB_MEM_Rk,
            MEMB_Membership_Id          AS membership_id,
            MEMB_Member_Id              AS member_id,
            MEMB_MembershipType_Code    AS membership_type,
            MEMB_Status_Code            AS status,
            MEMB_StartDate_Dt           AS start_date,
            MEMB_EndDate_Dt             AS end_date,
            MEMB_PriceNok_Dec           AS price_nok,
            MEMB_CreatedAt_Ts           AS created_at,
            MEMB_UpdatedAt_Ts           AS updated_at
        FROM {CATALOG}.silver.memberships
        WHERE __END_AT IS NULL
    """)
