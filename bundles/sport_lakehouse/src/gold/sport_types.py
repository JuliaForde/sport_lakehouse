from pyspark import pipelines as dp

CATALOG = spark.conf.get("catalog")


@dp.materialized_view(name=f"{CATALOG}.gold.sport_types")
def sport_types():
    return spark.sql(f"""
        SELECT
            SPTP_Rk,
            SPTP_SportType_Id           AS sport_type_id,
            SPTP_SportType_Name         AS sport_type_name,
            SPTP_SportMode_Code         AS sport_mode,
            SPTP_SeasonPeak_Code        AS season_peak,
            SPTP_IsOutdoor_Flag         AS is_outdoor,
            SPTP_PopularityWeight_Dec   AS popularity_weight
        FROM {CATALOG}.silver.sport_types
        WHERE __END_AT IS NULL
    """)
