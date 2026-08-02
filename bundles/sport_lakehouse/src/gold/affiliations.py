from pyspark import pipelines as dp

CATALOG = spark.conf.get("catalog")


@dp.materialized_view(name=f"{CATALOG}.gold.affiliations")
def affiliations():
    return spark.sql(f"""
        SELECT
            AFF_Rk,
            AFF_MEM_Rk,
            AFF_CLB_Rk,
            AFF_Affiliation_Id  AS affiliation_id,
            AFF_Member_Id       AS member_id,
            AFF_Club_Id         AS club_id,
            AFF_StartDate_Dt    AS start_date,
            AFF_EndDate_Dt      AS end_date,
            AFF_Reason_Desc     AS reason,
            AFF_CreatedAt_Ts    AS created_at,
            AFF_UpdatedAt_Ts    AS updated_at
        FROM {CATALOG}.silver.affiliations
        WHERE __END_AT IS NULL
    """)
