from pyspark import pipelines as dp
from utils import prepare_for_cdc, point_in_time_lookup, deduplicate
from configs import COMPETITIONS as CONFIG


@dp.temporary_view(name="competitions_prepared")
def competitions_prepared():
    df = spark.readStream.table("sport_lakehouse.bronze.competitions")
    df = deduplicate(df, CONFIG)
    df = prepare_for_cdc(df, CONFIG)
    for fk in CONFIG["fk_lookups"]:
        df = point_in_time_lookup(df=df, lookup_table=fk["lookup_table"],
            source_col=fk["source_col"], lookup_bk_col=fk["lookup_bk_col"],
            lookup_rk_col=fk["lookup_rk_col"], target_fk_col=fk["target_fk_col"],
            event_time_col=fk["event_time_col"], spark=spark)
    return df

dp.create_streaming_table("sport_lakehouse.silver.competitions")
dp.create_auto_cdc_flow(target="sport_lakehouse.silver.competitions",
    source="competitions_prepared", keys=["COMP_Rk"],
    sequence_by="_ingest_ts", stored_as_scd_type=2)
