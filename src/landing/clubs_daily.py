# Databricks notebook source
# DBTITLE 1,Cell 1
# clubs_daily — add new clubs daily
# - Uses widgets from 00_utils: run_date / volume / volume_factor (exposed as RUN_DATE, VOLUME, VOLUME_MULT)
# - Generates new clubs based on volume settings
# - Writes via merge_into() to avoid duplicates on reruns
#
# Prereq: Run 00_bootstrap_generate_sports_dataset first to create the base tables.

# COMMAND ----------

# DBTITLE 1,Cell 2
# Create/override the widgets that 00_utils actually reads
from datetime import date

dbutils.widgets.removeAll()
dbutils.widgets.text("run_date", date.today().isoformat())
dbutils.widgets.text("volume", "medium")  # low|medium|high
dbutils.widgets.text("volume_factor", "1.0")  # scales volume_mult

# COMMAND ----------

# DBTITLE 1,Cell 3
# MAGIC %run ./00_utils

# COMMAND ----------

# DBTITLE 1,Cell 4
# Define new_clubs based on volume
import random
from datetime import datetime

rnd = random.Random(seed_for("clubs"))

# Base daily volume for new clubs (scaled by VOLUME_MULT)
BASE_RANGES = {
    "low":    (0, 1),
    "medium": (0, 3),
    "high":   (1, 5),
}
lo, hi = BASE_RANGES.get(VOLUME, BASE_RANGES["medium"])
proposed_n = int(rnd.randint(lo, hi) * VOLUME_MULT)

# Realistic Norwegian club name patterns
club_prefixes = [
    "Fjell", "Strand", "Skog", "Fjord", "Dal", "Berg", "Ås", "Vang",
    "Gran", "Furumo", "Bjørke", "Lunde", "Sol", "Nord", "Sør", "Øst", "Vest",
    "Viking", "Ørn", "Elg", "Bjørn", "Ulv", "Hav", "Vind", "Storm"
]

club_types = ["IL", "SK", "Idrettslag", "Sportsklubb", "Turn", "IF"]

municipalities = [
    "Oslo", "Bergen", "Trondheim", "Stavanger", "Kristiansand", "Drammen",
    "Tromsø", "Ålesund", "Bodø", "Haugesund", "Tønsberg", "Sandefjord",
    "Skien", "Fredrikstad", "Sarpsborg", "Arendal", "Hamar", "Lillehammer"
]

county_map = {
    "Oslo": "Oslo",
    "Bergen": "Vestland",
    "Trondheim": "Trøndelag",
    "Stavanger": "Rogaland",
    "Kristiansand": "Agder",
    "Drammen": "Viken",
    "Tromsø": "Troms og Finnmark",
    "Ålesund": "Møre og Romsdal",
    "Bodø": "Nordland",
    "Haugesund": "Rogaland",
    "Tønsberg": "Vestfold og Telemark",
    "Sandefjord": "Vestfold og Telemark",
    "Skien": "Vestfold og Telemark",
    "Fredrikstad": "Viken",
    "Sarpsborg": "Viken",
    "Arendal": "Agder",
    "Hamar": "Innlandet",
    "Lillehammer": "Innlandet"
}

lat_long_map = {
    "Oslo": (59.91, 10.75),
    "Bergen": (60.39, 5.32),
    "Trondheim": (63.43, 10.39),
    "Stavanger": (58.97, 5.73),
    "Kristiansand": (58.15, 7.99),
    "Drammen": (59.74, 10.20),
    "Tromsø": (69.65, 18.96),
    "Ålesund": (62.47, 6.15),
    "Bodø": (67.28, 14.40),
    "Haugesund": (59.41, 5.27),
    "Tønsberg": (59.27, 10.41),
    "Sandefjord": (59.13, 10.22),
    "Skien": (59.21, 9.61),
    "Fredrikstad": (59.21, 10.93),
    "Sarpsborg": (59.28, 11.11),
    "Arendal": (58.46, 8.77),
    "Hamar": (60.79, 11.07),
    "Lillehammer": (61.11, 10.47)
}

# Get existing club IDs to avoid collisions
existing_club_ids = set()
try:
    existing = spark.table(tbl("clubs")).select("club_id").collect()
    existing_club_ids = {int(r["club_id"]) for r in existing}
except:
    pass  # Table doesn't exist yet

def get_next_club_id():
    if not existing_club_ids:
        return 10001
    return max(existing_club_ids) + 1

# Generate new clubs
new_clubs = []
for i in range(proposed_n):
    club_id = get_next_club_id() + i
    
    # Pick municipality and get corresponding county/coordinates
    municipality = rnd.choice(municipalities)
    county = county_map[municipality]
    base_lat, base_lon = lat_long_map[municipality]
    
    # Add some variation to coordinates
    lat = base_lat + rnd.uniform(-0.05, 0.05)
    lon = base_lon + rnd.uniform(-0.05, 0.05)
    
    # Generate club name - mix of patterns
    pattern = rnd.choice(["prefix_type", "muni_type", "muni_only"])
    if pattern == "prefix_type":
        club_name = f"{rnd.choice(club_prefixes)} {rnd.choice(club_types)}"
    elif pattern == "muni_type":
        club_name = f"{municipality} {rnd.choice(club_types)}"
    else:
        club_name = f"{municipality} {rnd.choice(['Idrettslag', 'Sportsklubb'])}"
    
    new_clubs.append({
        "club_id": club_id,
        "club_name": club_name,
        "club_type": rnd.choice(["multi-sport", "single-sport"]),
        "website": f"https://{club_name.lower().replace(' ', '')}.no",
        "street_address": f"{rnd.choice(['Idrettsveien', 'Stadionveien', 'Arenaallé', 'Hallveien'])} {rnd.randint(1,200)}",
        "postal_code": f"{rnd.randint(0,9999):04d}",
        "municipality_name": municipality,
        "county_name": county,
        "latitude": lat,
        "longitude": lon
    })

print(f"RUN_DATE={RUN_DATE.isoformat()}  VOLUME={VOLUME}  proposed_n={proposed_n}  new_clubs={len(new_clubs)}")
if new_clubs:
    print(f"Generated club names: {[c['club_name'] for c in new_clubs]}")
    existing_club_ids.update([c['club_id'] for c in new_clubs])

# COMMAND ----------

# DBTITLE 1,Cell 5
from pyspark.sql import functions as F

TABLE_ADDRESSES = "addresses"
TABLE_CLUBS = "clubs"
df_new_addresses = None  # Initialize at notebook level for Cell 2
df_new_clubs = None  # Initialize at notebook level for Cell 2

# Read current addresses schema to detect street column name
addr_df = spark.table(tbl(TABLE_ADDRESSES))
target_cols = [f.name for f in addr_df.schema.fields]
street_col = "street_address" if "street_address" in target_cols else ("street" if "street" in target_cols else None)
if street_col is None:
    raise Exception("addresses table missing street column (expected 'street_address' or 'street').")

n_new = len(new_clubs)
if n_new == 0:
    dbutils.notebook.exit("No new clubs today.")

# 1) Allocate NEW address ids (owner="clubs" keeps these separate from the
#    member addresses that 06_addresses_daily allocates on the same day)
alloc_addr = allocate_ids(TABLE_ADDRESSES, "address_id", n_new, owner="clubs")
start_addr = alloc_addr["start_id"]
end_addr = alloc_addr["end_id"]
new_addr_ids = list(range(start_addr + 1, end_addr + 1))

# 2) Build NEW address rows for clubs (using your schema names).
#    Start from ALL target columns as None (like 06_addresses_daily), then fill
#    what we know. This guarantees every row matches the live addresses schema.
from datetime import datetime
now_ts = datetime.utcnow()

addr_rows = []
for i, club in enumerate(new_clubs):
    aid = int(new_addr_ids[i])

    municipality = club.get("municipality_name") or club.get("municipality") or None
    county = club.get("county_name") or club.get("county") or None
    postal = club.get("postal_code") or None

    row = {c: None for c in target_cols}  # all columns, default None
    row["address_id"] = aid
    row[street_col] = club.get("street_address") or club.get("street") or club.get("address_line") or f"{club['club_name']} klubbhus"
    if "postal_code" in target_cols:        row["postal_code"] = postal
    if "municipality_name" in target_cols:  row["municipality_name"] = municipality
    if "county_name" in target_cols:        row["county_name"] = county
    if "latitude" in target_cols:           row["latitude"] = club.get("latitude")
    if "longitude" in target_cols:          row["longitude"] = club.get("longitude")
    if "country" in target_cols:            row["country"] = "Norway"
    if "created_at" in target_cols:         row["created_at"] = now_ts
    if "updated_at" in target_cols:         row["updated_at"] = now_ts

    addr_rows.append(row)

# Pass the live table schema so types match bootstrap exactly (no NullType drift
# on nullable fields like latitude/longitude/postal_code).
df_new_addresses = spark.createDataFrame(addr_rows, schema=addr_df.schema)

# 3) Merge into addresses (Delta) and record the allocation (idempotent rerun)
merge_into(TABLE_ADDRESSES, df_new_addresses, ["address_id"])
record_state(TABLE_ADDRESSES, start_addr, end_addr, n_new, notes=f"Inserted {n_new} club addresses", owner="clubs")

# 4) Attach allocated address_id to each club row and build clubs DF
clubs_rows = []
for i, club in enumerate(new_clubs):
    clubs_rows.append({
        "club_id": int(club["club_id"]),
        "club_name": club["club_name"],
        "address_id": int(new_addr_ids[i]),  # IMPORTANT: brand-new address id
        "club_type": club.get("club_type"),
        "website": club.get("website"),
        "created_at": RUN_DATE,  # if you have this column in clubs
    })

df_new_clubs = spark.createDataFrame(clubs_rows)

# 5) Merge into clubs
merge_into(TABLE_CLUBS, df_new_clubs, ["club_id"])

print(f"Created {n_new} new clubs and their addresses for {RUN_DATE.isoformat()}")
display(df_new_clubs)
# COMMAND ----------

# DBTITLE 1,Cell 6
# Export to landing (overwrite per run_date partition — idempotent).
# Club addresses go to .../addresses/<date>/clubs/ so they coexist with the
# member addresses 06_addresses_daily writes to .../addresses/<date>/members/.
export_to_landing(TABLE_ADDRESSES, df_new_addresses, owner="clubs")
export_to_landing(TABLE_CLUBS, df_new_clubs)
