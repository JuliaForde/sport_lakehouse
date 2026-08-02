from pyspark import pipelines as dp

CATALOG = spark.conf.get("catalog")


@dp.materialized_view(name=f"{CATALOG}.gold.competition_waitlist")
def competition_waitlist():
    return spark.sql(f"""
        SELECT
            COWL_Rk,
            COWL_COMP_Rk,
            COWL_MEM_Rk,
            COWL_CLB_Rk,
            COWL_PART_Rk,
            COWL_Waitlist_Id                AS waitlist_id,
            COWL_Competition_Id             AS competition_id,
            COWL_Member_Id                  AS member_id,
            COWL_Club_Id                    AS club_id,
            COWL_Status_Code                AS status,
            COWL_PromotedParticipation_Id   AS promoted_participation_id,
            COWL_AddedAt_Ts                 AS added_at,
            COWL_UpdatedAt_Ts               AS updated_at
        FROM {CATALOG}.silver.competition_waitlist
        WHERE __END_AT IS NULL
    """)
