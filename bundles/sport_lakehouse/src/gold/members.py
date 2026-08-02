from pyspark import pipelines as dp

CATALOG = spark.conf.get("catalog")


@dp.materialized_view(name=f"{CATALOG}.gold.members")
def members():
    return spark.sql(f"""
        SELECT
            MEM_Rk,
            MEM_ADR_Rk,
            MEM_Member_Id               AS member_id,
            MEM_Address_Id              AS address_id,
            MEM_FirstName_Name          AS first_name,
            MEM_LastName_Name           AS last_name,
            MEM_Gender_Code             AS gender,
            MEM_Nationality_Name        AS nationality,
            MEM_CountryOfBirth_Name     AS country_of_birth,
            MEM_HasRelocated_Flag       AS has_relocated,
            MEM_RelocationYear_No       AS relocation_year,
            MEM_Email_Desc              AS email,
            MEM_Phone_Desc              AS phone,
            MEM_BirthDate_Dt            AS birth_date,
            MEM_AcceptsMarketing_Flag   AS accepts_marketing,
            MEM_CreatedAt_Ts            AS created_at
        FROM {CATALOG}.silver.members
        WHERE __END_AT IS NULL
    """)
