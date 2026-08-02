from pyspark import pipelines as dp

CATALOG = spark.conf.get("catalog")


@dp.materialized_view(name=f"{CATALOG}.gold.results")
def results():
    return spark.sql(f"""
        SELECT
            RES_Rk,
            RES_PART_Rk,
            RES_Result_Id           AS result_id,
            RES_Participation_Id    AS participation_id,
            RES_Position_No         AS position,
            RES_Score_No            AS score,
            RES_TimeSeconds_No      AS time_seconds,
            RES_Notes_Desc          AS notes,
            RES_IsOfficial_Flag     AS is_official,
            RES_RecordedAt_Ts       AS recorded_at,
            RES_CorrectedAt_Ts      AS corrected_at
        FROM {CATALOG}.silver.results
        WHERE __END_AT IS NULL
    """)
