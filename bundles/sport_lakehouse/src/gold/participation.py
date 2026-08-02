from pyspark import pipelines as dp

CATALOG = spark.conf.get("catalog")


@dp.materialized_view(name=f"{CATALOG}.gold.participation")
def participation():
    return spark.sql(f"""
        SELECT
            PART_Rk,
            PART_COMP_Rk,
            PART_MEM_Rk,
            PART_CLB_Rk,
            PART_Participation_Id   AS participation_id,
            PART_Competition_Id     AS competition_id,
            PART_Member_Id          AS member_id,
            PART_Club_Id            AS club_id,
            PART_Status_Code        AS status,
            PART_RegisteredAt_Ts    AS registered_at,
            PART_StatusUpdatedAt_Ts AS status_updated_at,
            PART_Source_Code        AS source,
            PART_BibNumber_No       AS bib_number
        FROM {CATALOG}.silver.participation
        WHERE __END_AT IS NULL
    """)
