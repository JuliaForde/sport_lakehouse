# Databricks notebook source
# 06_addresses_daily — add new addresses daily (integrated with 00_utils)
# - Uses widgets from 00_utils: run_date / volume / volume_factor (exposed as RUN_DATE, VOLUME, VOLUME_MULT)
# - Uses sim_state via allocate_ids()/record_state() to be idempotent per RUN_DATE
# - Writes via merge_into() to avoid duplicates on reruns
#
# Prereq: Run 00_bootstrap_generate_sports_dataset first to create the base tables.

# COMMAND ----------

# Create/override the widgets that 00_utils actually reads
from datetime import date

dbutils.widgets.text("run_date",date.today().isoformat() )          # <- Monday
dbutils.widgets.text("volume", "medium")                # low|medium|high
dbutils.widgets.text("volume_factor", "1.0")            # scales volume_mult

# COMMAND ----------

# MAGIC %run ./00_utils

# COMMAND ----------

import random
from datetime import datetime
from pyspark.sql import functions as F

TABLE = "addresses"
ID_COL = "address_id"

rnd = random.Random(seed_for(TABLE))

# Base daily volume (scaled by VOLUME_MULT)
BASE_RANGES = {
    "low":    (0, 5),
    "medium": (0, 25),
    "high":   (5, 80),
}
lo, hi = BASE_RANGES.get(VOLUME, BASE_RANGES["medium"])
proposed_n = int(rnd.randint(lo, hi) * VOLUME_MULT)

alloc = allocate_ids(TABLE, ID_COL, proposed_n, owner="members")
start_id, n_new, already = alloc["start_id"], alloc["n_rows"], alloc["already_ran"]

print(f"RUN_DATE={RUN_DATE.isoformat()}  VOLUME={VOLUME}  proposed_n={proposed_n}  n_new={n_new}  already_ran={already}")

# COMMAND ----------

# DBTITLE 1,Cell 5
# If rerun for same RUN_DATE, do nothing (idempotent)
df_new_addresses = None  # Initialize at notebook level

if already:
    print(f"Addresses already generated for {RUN_DATE.isoformat()} (sim_state). Skipping writes.")
else:
    # Load existing addresses (defines the target schema)
    try:
        addr_df = spark.table(tbl(TABLE))
    except Exception as e:
        raise Exception("Missing table addresses. Run 00_bootstrap_generate_sports_dataset first.") from e

    target_schema = addr_df.schema
    target_cols = [f.name for f in target_schema.fields]

    # Handle schema differences between versions:
    # - bootstrap uses 'street_address' (not 'street')
    # - some versions may include 'postal_place'
    street_col = "street_address" if "street_address" in target_cols else ("street" if "street" in target_cols else None)
    if street_col is None:
        raise Exception("addresses table is missing a street column (expected 'street_address' or 'street').")

    required = {ID_COL, street_col, "postal_code", "municipality_name", "county_name", "latitude", "longitude"}
    missing_required = sorted(list(required - set(target_cols)))
    if missing_required:
        raise Exception(f"addresses table is missing required columns: {missing_required}")

    if n_new == 0:
        print("No new addresses today (n_new=0).")
        record_state(TABLE, start_id, start_id + n_new, n_new, notes="Inserted 0 addresses", owner="members")
    else:
        base = (addr_df
                .select(ID_COL, "postal_code", "municipality_name", "county_name", "latitude", "longitude",
                        *([c for c in ["postal_place"] if c in target_cols]))
                .where(F.col("municipality_name").isNotNull())
                .where(F.col("county_name").isNotNull())
               ).cache()

        muni_stats = (base
            .groupBy("municipality_name","county_name")
            .agg(
                F.count("*").alias("cnt"),
                (F.first("postal_place", ignorenulls=True).alias("postal_place") if "postal_place" in target_cols else F.lit(None).alias("postal_place")),
                F.avg("latitude").alias("lat_mu"),
                F.avg("longitude").alias("lon_mu"),
                F.collect_set("postal_code").alias("postal_codes")
            )
        ).collect()

        if not muni_stats:
            raise RuntimeError("addresses table has no usable municipality/county rows; run bootstrap first.")

        weights = [max(1, int(r["cnt"])) for r in muni_stats]
        total_w = float(sum(weights))

        def pick_muni():
            x = rnd.random() * total_w
            s = 0.0
            for r, w in zip(muni_stats, weights):
                s += w
                if s >= x:
                    return r
            return muni_stats[-1]

        street_names = [
    # Klassiske
    "Storgata","Kirkeveien","Parkveien","Skoleveien","Bakkegata","Havnegata","Fjordveien",
    "Granliveien","Lyngveien","Furulia","Solbakken","Åsveien","Industriveien","Strandgata",
    "Elveveien","Ringveien","Jernbaneveien","Høgda","Furuveien","Bjørkeveien",
    "Eventyrveien","Skiveien","Godalsgate","Dælenggata","Sørsidegata","Turveien",
    "Seinesveien","Trosterudveien","Vikegata",

    # Natur
    "Fjellveien","Skogveien","Havreveien","Engveien","Markveien","Heiveien",
    "Myrveien","Lindelia","Granåsen","Bjørkåsen","Furumoen","Eikelund",
    "Solheimveien","Sjøveien","Himmelstien","Nordlia","Sydlia","Vestlia","Østlia",
    "Blåbærstien","Bringebærstien","Moseveien","Sletteveien","Dalsveien",
    "Bergveien","Fossveien","Furubakken","Eikebakken","Løvåsen",

    # Boligområder
    "Hageveien","Hagestien","Hagelia","Hagelunden",
    "Torggata","Sentrumsgata","Villaveien","Tunveien","Tunlia",
    "Toppenveien","Toppenlia","Bakkenveien","Bakkelia",
    "Skogstien","Skogslia","Skogstunet",

    # Mer urbane
    "Kongens gate","Dronningens gate","Prinsens gate","Olav Kyrres gate",
    "Karl Johans gate","Nedre gate","Øvre gate","Langgata",
    "Torget","Bryggegata","Kaigata","Havneveien",
    "Rådhusgata","Bankgata","Stasjonsveien","Terminalveien",

    # Litt variasjon
    "Fjelltoppen","Soltoppen","Nordtoppen","Sørtoppen",
    "Granittveien","Skiferveien","Marmorveien",
    "Solsiden","Måneskinnsveien","Stjerneveien",
    "Regnbuestien","Sommerveien","Vinterveien","Høstveien","Vårveien",

    # Flere realistiske
    "Åsaveien","Åsenveien","Haugenveien","Haugstien","Hauglia",
    "Nygata","Nyveien","Gamleveien","Gamlebygata",
    "Kverneveien","Mølleveien","Sagveien",
    "Tyttebærveien","Blåklokkeveien","Prestekrageveien",
    "Hasselveien","Ospelia","Rognveien",
    "Kornveien","Bygdeveien","Landeveien",
    "Industrigata","Verkstedveien","Fabrikkveien",
    "Skytterveien","Idrettsveien","Arenaallé",
    "Stadionveien","Turnveien","Svømmehallveien"
]

        def new_postal_code():
            return f"{rnd.randint(0, 9999):04d}"

        rows = []
        now_ts = datetime.utcnow()

        for i in range(n_new):
            aid = int(start_id) + i
            m = pick_muni()

            street_value = f"{rnd.choice(street_names)} {rnd.randint(1, 220)}"

            # 10% chance of a brand-new postal code; otherwise reuse existing ones in that municipality
            pcs = [p for p in (m["postal_codes"] or []) if p is not None]
            postal_code = new_postal_code() if rnd.random() < 0.10 or not pcs else str(rnd.choice(pcs))

            lat_mu = float(m["lat_mu"])
            lon_mu = float(m["lon_mu"])
            # jitter degrees ~ km/111; 0.002..0.01 deg ~ 0.2..1.1 km
            j = rnd.uniform(0.002, 0.01)
            lat = lat_mu + rnd.uniform(-j, j)
            lon = lon_mu + rnd.uniform(-j, j)

            # Start with all target columns as None, then fill what we know
            row = {c: None for c in target_cols}

            row[ID_COL] = aid
            row[street_col] = street_value
            row["postal_code"] = str(postal_code)
            row["municipality_name"] = m["municipality_name"]
            row["county_name"] = m["county_name"]
            row["latitude"] = float(lat)
            row["longitude"] = float(lon)

            # Optional columns if present
            if "postal_place" in target_cols:
                row["postal_place"] = m["postal_place"]  # may be None; that's fine
            if "country" in target_cols and row["country"] is None:
                row["country"] = "Norway"

            # Common timestamp columns
            if "created_at" in target_cols and row["created_at"] is None:
                row["created_at"] = now_ts
            if "updated_at" in target_cols and row["updated_at"] is None:
                row["updated_at"] = now_ts

            rows.append(row)

        df_new = spark.createDataFrame(rows, schema=target_schema)
        df_new_addresses = df_new  # Store at notebook level for Cell 6

        merge_into(TABLE, df_new, [ID_COL])
        record_state(TABLE, start_id, start_id + n_new, n_new, notes=f"Inserted {n_new} addresses", owner="members")

        print(f"Upserted {n_new} addresses into {tbl(TABLE)}")
        display(df_new.limit(20))

# COMMAND ----------

# DBTITLE 1,Cell 6
# Export new addresses to landing volume (overwrite per run_date partition — idempotent).
# owner="members" writes to .../addresses/<date>/members/ so it never clobbers the
# club addresses that clubs_daily writes to .../addresses/<date>/clubs/.
export_to_landing(TABLE, df_new_addresses, owner="members")