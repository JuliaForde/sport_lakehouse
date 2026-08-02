from pyspark import pipelines as dp

CATALOG = spark.conf.get("catalog")


@dp.materialized_view(name=f"{CATALOG}.gold.addresses")
def addresses():
    return spark.sql(f"""
        SELECT
            ADR_Rk,
            ADR_Address_Id          AS address_id,
            ADR_Country_Code        AS country,
            ADR_County_Name         AS county,
            ADR_Municipality_Name   AS municipality,
            ADR_PostalCode_Code     AS postal_code,
            ADR_StreetAddress_Desc  AS street_address,
            ADR_Latitude_Dec        AS latitude,
            ADR_Longitude_Dec       AS longitude
        FROM {CATALOG}.silver.addresses
        WHERE __END_AT IS NULL
    """)
