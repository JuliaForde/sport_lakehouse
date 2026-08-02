from pyspark import pipelines as dp

CATALOG = spark.conf.get("catalog")


@dp.materialized_view(name=f"{CATALOG}.gold.club_sports")
def club_sports():
    return spark.sql(f"""
        SELECT
            CLSP_Rk,
            CLSP_CLB_Rk,
            CLSP_SPTP_Rk,
            CLSP_Club_Id        AS club_id,
            CLSP_SportType_Id   AS sport_type_id
        FROM {CATALOG}.silver.club_sports
        WHERE __END_AT IS NULL
    """)
