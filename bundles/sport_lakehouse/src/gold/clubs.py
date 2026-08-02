from pyspark import pipelines as dp

CATALOG = spark.conf.get("catalog")


@dp.materialized_view(name=f"{CATALOG}.gold.clubs")
def clubs():
    return spark.sql(f"""
        SELECT
            CLB_Rk,
            CLB_ADR_Rk,
            CLB_Club_Id         AS club_id,
            CLB_Address_Id      AS address_id,
            CLB_Club_Name       AS club_name,
            CLB_ClubType_Code   AS club_type,
            CLB_Website_Desc    AS website,
            CLB_FoundedYear_No  AS founded_year,
            CLB_Division_Code   AS division,
            CLB_IsActive_Flag   AS is_active,
            CLB_CreatedAt_Ts    AS created_at
        FROM {CATALOG}.silver.clubs
        WHERE __END_AT IS NULL
    """)
