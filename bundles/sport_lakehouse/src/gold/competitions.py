from pyspark import pipelines as dp

CATALOG = spark.conf.get("catalog")


@dp.materialized_view(name=f"{CATALOG}.gold.competitions")
def competitions():
    return spark.sql(f"""
        SELECT
            COMP_Rk,
            COMP_ADR_Rk,
            COMP_CLB_Rk,
            COMP_SPTP_Rk,
            COMP_Competition_Id             AS competition_id,
            COMP_SportType_Id               AS sport_type_id,
            COMP_HostClub_Id                AS host_club_id,
            COMP_Address_Id                 AS address_id,
            COMP_Competition_Name           AS competition_name,
            COMP_Venue_Desc                 AS venue,
            COMP_Level_Code                 AS level,
            COMP_Status_Code                AS status,
            COMP_Capacity_No                AS capacity,
            COMP_StartDate_Dt               AS start_date,
            COMP_EndDate_Dt                 AS end_date,
            COMP_RegistrationDeadline_Dt    AS registration_deadline,
            COMP_EntryFeeNok_Dec            AS entry_fee_nok,
            COMP_PrizePoolNok_Dec           AS prize_pool_nok,
            COMP_IsOutdoor_Flag             AS is_outdoor,
            COMP_CreatedAt_Ts               AS created_at,
            COMP_UpdatedAt_Ts               AS updated_at
        FROM {CATALOG}.silver.competitions
        WHERE __END_AT IS NULL
    """)
