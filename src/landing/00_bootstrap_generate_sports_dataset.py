# Databricks notebook source
# 00_bootstrap_generate_sports_dataset — FULL base dataset generator (creates all tables)
# Target: demo_data.sports (edit at top if needed)

# COMMAND ----------

# MAGIC %pip install Faker

# COMMAND ----------

import random
from datetime import date, timedelta, datetime
from pyspark.sql import functions as F
from faker import Faker

# === knobs ===
RESET_SCHEMA = True     # True = drop & recreate tables (recommended if you deleted them)
ADD_CONSTRAINTS = False # keep False unless you want UC constraints

TARGET_CATALOG = "demo_data"
TARGET_SCHEMA  = "sports"

def tbl(name: str) -> str:
    return f"`{TARGET_CATALOG}`.`{TARGET_SCHEMA}`.`{name}`"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{TARGET_CATALOG}`.`{TARGET_SCHEMA}`")

# CONFIG (40% more rows + ~1 year more history vs previous base)
SEED = 42
random.seed(SEED)

N_MEMBERS      = 124_567
N_CLUBS        = 220
N_COMPETITIONS = 920

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField,
    LongType, IntegerType, StringType, BooleanType,
    DateType, TimestampType
)

aff_schema = StructType([
    StructField("affiliation_id", LongType(), False),
    StructField("member_id",      LongType(), False),
    StructField("club_id",        LongType(), False),
    StructField("start_date",     DateType(), False),
    StructField("end_date",       DateType(), True),          # <-- all None at bootstrap
    StructField("reason",         StringType(), True),
    StructField("created_at",     TimestampType(), True),
    StructField("updated_at",     TimestampType(), True),
])

part_schema = StructType([
    StructField("participation_id", LongType(), False),
    StructField("competition_id",   LongType(), False),
    StructField("member_id",        LongType(), False),
    StructField("club_id",          LongType(), True),
    StructField("registered_at",    TimestampType(), True),
    StructField("status",           StringType(), True),
    StructField("status_updated_at",TimestampType(), True),
    StructField("source",           StringType(), True),
    StructField("bib_number",       IntegerType(), True),     # <-- all None at bootstrap
])

wait_schema = StructType([
    StructField("waitlist_id",                 LongType(), False),
    StructField("competition_id",             LongType(), False),
    StructField("member_id",                  LongType(), False),
    StructField("club_id",                    LongType(), True),
    StructField("added_at",                   TimestampType(), True),
    StructField("status",                     StringType(), True),
    StructField("promoted_participation_id",  LongType(), True),  # <-- all None at bootstrap
    StructField("updated_at",                 TimestampType(), True),
])

results_schema = StructType([
    StructField("result_id",        LongType(), False),
    StructField("participation_id", LongType(), False),
    StructField("position",         IntegerType(), True),
    StructField("score",            IntegerType(), True),
    StructField("time_seconds",     IntegerType(), True),
    StructField("notes",            StringType(), True),
    StructField("is_official",      BooleanType(), True),
    StructField("recorded_at",      TimestampType(), True),
    StructField("corrected_at",     TimestampType(), True),   # <-- all None at bootstrap
])

# COMMAND ----------

# Helper: drop tables (safe)
if RESET_SCHEMA:
    for t in [
        "membership_payments","memberships","competition_waitlist","results","participation",
        "competitions","affiliations","members","club_sports","clubs","sport_types","addresses"
    ]:
        spark.sql(f"DROP TABLE IF EXISTS {tbl(t)}")

# COMMAND ----------

# Helper: write tables (Delta)
def save_table(df, name: str):
    (df.write.format("delta")
       .mode("overwrite")
       .option("overwriteSchema","true")
       .saveAsTable(tbl(name)))

# COMMAND ----------

# ---- Addresses ----
# Simple but realistic Norwegian geo backbone (municipality/county + approximate coordinates).
# We keep enough addresses to support member & club locality logic.

fake = Faker("no_NO")

municipalities = [
  # Oslo-regionen
  ("Oslo","Oslo",59.9139,10.7522),
  ("Bærum","Viken",59.8943,10.5268),
  ("Lørenskog","Viken",59.9167,10.9667),
  ("Lillestrøm","Viken",59.9553,11.0496),
  ("Asker","Viken",59.8333,10.4333),
  ("Ås","Viken",59.6619,10.7783),
  ("Ski","Viken",59.7219,10.8363),
  ("Jessheim","Viken",60.1436,11.1736),
  ("Moss","Viken",59.4344,10.6578),
  ("Sarpsborg","Viken",59.2839,11.1097),
  ("Fredrikstad","Viken",59.2181,10.9298),
  ("Halden","Viken",59.1228,11.3872),
  ("Drammen","Viken",59.7439,10.2045),
  ("Kongsberg","Viken",59.6667,9.6500),
  ("Hønefoss","Viken",60.1667,10.2500),
  ("Sandvika","Viken",59.8908,10.5264),
  # Innlandet
  ("Hamar","Innlandet",60.7945,11.0680),
  ("Lillehammer","Innlandet",61.1153,10.4662),
  ("Gjøvik","Innlandet",60.7957,10.6916),
  ("Brumunddal","Innlandet",60.8833,10.9333),
  ("Elverum","Innlandet",60.8833,11.5667),
  ("Kongsvinger","Innlandet",60.1917,12.0028),
  ("Moelv","Innlandet",60.9333,10.7000),
  ("Fagernes","Innlandet",60.9833,9.2333),
  ("Otta","Innlandet",61.7667,9.5333),
  ("Tynset","Innlandet",62.2833,10.7833),
  ("Røros","Innlandet",62.5744,11.3853),
  # Vestfold og Telemark
  ("Tønsberg","Vestfold og Telemark",59.2675,10.4074),
  ("Sandefjord","Vestfold og Telemark",59.1317,10.2167),
  ("Larvik","Vestfold og Telemark",59.0542,10.0286),
  ("Horten","Vestfold og Telemark",59.4167,10.4833),
  ("Holmestrand","Vestfold og Telemark",59.4917,10.3167),
  ("Skien","Vestfold og Telemark",59.2096,9.6090),
  ("Porsgrunn","Vestfold og Telemark",59.1406,9.6556),
  ("Notodden","Vestfold og Telemark",59.5603,9.2558),
  ("Kragerø","Vestfold og Telemark",58.8667,9.4167),
  ("Bø","Vestfold og Telemark",59.4167,9.0667),
  # Agder
  ("Kristiansand","Agder",58.1467,7.9956),
  ("Arendal","Agder",58.4615,8.7727),
  ("Grimstad","Agder",58.3403,8.5931),
  ("Mandal","Agder",58.0292,7.4608),
  ("Farsund","Agder",58.0944,6.7986),
  ("Flekkefjord","Agder",58.2972,6.6636),
  ("Lyngdal","Agder",58.1333,7.0667),
  ("Lillesand","Agder",58.2500,8.3833),
  ("Vennesla","Agder",58.1667,7.9667),
  # Rogaland
  ("Stavanger","Rogaland",58.9690,5.7331),
  ("Sandnes","Rogaland",58.8524,5.7352),
  ("Haugesund","Rogaland",59.4138,5.2680),
  ("Egersund","Rogaland",58.4508,6.0028),
  ("Bryne","Rogaland",58.7333,5.6500),
  ("Kopervik","Rogaland",59.2833,5.3000),
  ("Sauda","Rogaland",59.6500,6.3500),
  ("Jørpeland","Rogaland",59.0167,6.0500),
  ("Randaberg","Rogaland",59.0000,5.6167),
  ("Klepp","Rogaland",58.7667,5.6333),
  # Vestland
  ("Bergen","Vestland",60.3913,5.3221),
  ("Åsane","Vestland",60.4667,5.3333),
  ("Askøy","Vestland",60.3833,5.1667),
  ("Os","Vestland",60.1833,5.4667),
  ("Stord","Vestland",59.7833,5.5000),
  ("Odda","Vestland",60.0667,6.5500),
  ("Voss","Vestland",60.6267,6.4175),
  ("Florø","Vestland",61.5992,5.0331),
  ("Sogndal","Vestland",61.2292,7.1000),
  ("Førde","Vestland",61.4531,5.8572),
  ("Leirvik","Vestland",59.7833,5.5000),
  ("Stryn","Vestland",61.9039,6.7228),
  ("Eid","Vestland",61.8000,5.9333),
  # Møre og Romsdal
  ("Ålesund","Møre og Romsdal",62.4722,6.1549),
  ("Molde","Møre og Romsdal",62.7375,7.1591),
  ("Kristiansund","Møre og Romsdal",63.1105,7.7278),
  ("Ulsteinvik","Møre og Romsdal",62.3500,5.8500),
  ("Sunndalsøra","Møre og Romsdal",62.6833,8.5500),
  ("Volda","Møre og Romsdal",62.1500,6.0833),
  ("Ørsta","Møre og Romsdal",62.2000,6.1333),
  ("Eidsvåg","Møre og Romsdal",62.7833,7.8167),
  ("Vestnes","Møre og Romsdal",62.6333,7.0500),
  ("Fosnavåg","Møre og Romsdal",62.3333,5.6333),
  # Trøndelag
  ("Trondheim","Trøndelag",63.4305,10.3951),
  ("Steinkjer","Trøndelag",64.0167,11.4833),
  ("Namsos","Trøndelag",64.4667,11.5000),
  ("Verdal","Trøndelag",63.7833,11.4833),
  ("Levanger","Trøndelag",63.7500,11.3000),
  ("Stjørdalshalsen","Trøndelag",63.4667,10.9333),
  ("Åfjord","Trøndelag",63.9500,10.2000),
  ("Røros","Trøndelag",62.5744,11.3853),
  ("Melhus","Trøndelag",63.2833,10.2667),
  ("Orkanger","Trøndelag",63.3167,9.8500),
  ("Brekstad","Trøndelag",63.7000,9.6833),
  ("Oppdal","Trøndelag",62.5917,9.6833),
  ("Røyrvik","Trøndelag",64.8667,13.5500),
  ("Grong","Trøndelag",64.4667,12.3167),
  ("Inderøy","Trøndelag",63.9833,11.2333),
  # Nordland
  ("Bodø","Nordland",67.2804,14.4049),
  ("Narvik","Nordland",68.4386,17.4278),
  ("Mo i Rana","Nordland",66.3167,14.1500),
  ("Mosjøen","Nordland",65.8344,13.1961),
  ("Sandnessjøen","Nordland",66.0167,12.6333),
  ("Brønnøysund","Nordland",65.4742,12.2136),
  ("Fauske","Nordland",67.2667,15.3833),
  ("Lødingen","Nordland",68.4167,16.0000),
  ("Sortland","Nordland",68.6944,15.4167),
  ("Svolvær","Nordland",68.2347,14.5656),
  ("Leknes","Nordland",68.1453,13.6072),
  ("Stokmarknes","Nordland",68.5667,14.9167),
  ("Ørnes","Nordland",66.8500,13.7000),
  ("Røst","Nordland",67.5236,12.1019),
  # Troms og Finnmark
  ("Tromsø","Troms og Finnmark",69.6492,18.9553),
  ("Harstad","Troms og Finnmark",68.7983,16.5417),
  ("Alta","Troms og Finnmark",69.9689,23.2716),
  ("Hammerfest","Troms og Finnmark",70.6634,23.6820),
  ("Vadsø","Troms og Finnmark",70.0733,29.7508),
  ("Vardø","Troms og Finnmark",70.3714,31.1106),
  ("Kirkenes","Troms og Finnmark",69.7267,30.0453),
  ("Finnsnes","Troms og Finnmark",69.2333,17.9833),
  ("Storslett","Troms og Finnmark",69.7667,21.0333),
  ("Skjervøy","Troms og Finnmark",70.0333,20.9833),
  ("Lakselv","Troms og Finnmark",70.0500,24.9667),
  ("Kautokeino","Troms og Finnmark",69.0167,23.0333),
  ("Karasjok","Troms og Finnmark",69.4667,25.5000),
  ("Honningsvåg","Troms og Finnmark",70.9833,25.9667),
  ("Båtsfjord","Troms og Finnmark",70.6333,29.7167),
  ("Berlevåg","Troms og Finnmark",70.8583,29.0917),
  ("Tana","Troms og Finnmark",70.2000,28.1833),
  ("Mehamn","Troms og Finnmark",71.0333,27.8500),
  ("Kjøllefjord","Troms og Finnmark",70.9500,27.3500),
  ("Lebesby","Troms og Finnmark",70.5500,26.9000),
  # Svalbard
  ("Longyearbyen","Svalbard",78.2232,15.6267),
  # Diverse mindre kommunar
  ("Jessheim","Viken",60.1436,11.1736),
  ("Lillestrøm","Viken",59.9553,11.0496),
  ("Mysen","Viken",59.5500,11.3333),
  ("Askim","Viken",59.6167,11.1667),
  ("Eidsberg","Viken",59.5333,11.2333),
  ("Rakkestad","Viken",59.4000,11.3500),
  ("Rygge","Viken",59.3833,10.7333),
  ("Råde","Viken",59.3333,10.8333),
  ("Hvaler","Viken",59.0833,10.9667),
  ("Spydeberg","Viken",59.6167,11.0833),
  ("Vestby","Viken",59.6000,10.7500),
  ("Frogn","Viken",59.6833,10.6500),
  ("Nesodden","Viken",59.8167,10.6667),
  ("Oppegård","Viken",59.8000,10.8000),
  ("Enebakk","Viken",59.7500,11.0000),
  ("Rælingen","Viken",59.9167,11.0333),
  ("Nittedal","Viken",60.0500,10.8667),
  ("Nannestad","Viken",60.2333,11.0833),
  ("Eidsvoll","Viken",60.3333,11.2500),
  ("Hurdal","Viken",60.3667,11.0667),
  ("Gjerdrum","Viken",60.1500,11.0667),
  ("Ullensaker","Viken",60.1667,11.1667),
  ("Aurskog-Høland","Viken",59.9167,11.5000),
  ("Sørum","Viken",60.0167,11.2167),
  ("Fet","Viken",59.9667,11.1833),
  ("Skedsmo","Viken",59.9833,11.0500),
  ("Nøtterøy","Vestfold og Telemark",59.2167,10.4167),
  ("Stokke","Vestfold og Telemark",59.2833,10.3000),
  ("Andebu","Vestfold og Telemark",59.2500,10.1500),
  ("Tjøme","Vestfold og Telemark",59.1333,10.4333),
  ("Lardal","Vestfold og Telemark",59.3500,10.0167),
  ("Siljan","Vestfold og Telemark",59.2833,9.7333),
  ("Drangedal","Vestfold og Telemark",59.0833,9.0667),
  ("Nome","Vestfold og Telemark",59.3667,9.3833),
  ("Hjartdal","Vestfold og Telemark",59.6167,8.7500),
  ("Seljord","Vestfold og Telemark",59.4833,8.6500),
  ("Kviteseid","Vestfold og Telemark",59.4000,8.4833),
  ("Nissedal","Vestfold og Telemark",59.1500,8.5000),
  ("Fyresdal","Vestfold og Telemark",59.1667,8.0833),
  ("Tokke","Vestfold og Telemark",59.4333,8.1500),
  ("Vinje","Vestfold og Telemark",59.5667,7.9667),
  ("Flå","Viken",60.4167,9.2833),
  ("Nes","Viken",60.5667,9.4333),
  ("Gol","Viken",60.7000,9.0000),
  ("Hemsedal","Viken",60.8667,8.5500),
  ("Ål","Viken",60.6333,8.5667),
]

# more addresses than before, but still manageable
N_ADDRESSES = 28_000

def jitter(base, scale=0.06):
    return base + random.uniform(-scale, scale)

addresses = []
for aid in range(1, N_ADDRESSES+1):
    muni, county, lat, lon = random.choice(municipalities)
    # keep coordinates close to municipality center
    addresses.append({
        "address_id": aid,
        "street_address": fake.street_address(),
        "postal_code": f"{random.randint(1, 9999):04d}",
        "municipality_name": muni,
        "county_name": county,
        "country": "Norway",
        "latitude": float(jitter(lat, 0.07)),
        "longitude": float(jitter(lon, 0.12))
    })

addresses_df = spark.createDataFrame(addresses)
save_table(addresses_df, "addresses")

# Lookup maps for locality logic
addr_geo = {r["address_id"]: (r["municipality_name"], r["county_name"]) for r in addresses_df.select("address_id","municipality_name","county_name").collect()}
addr_ids_by_muni = {}
addr_ids_by_county = {}
for aid, (m,c) in addr_geo.items():
    addr_ids_by_muni.setdefault(m, []).append(aid)
    addr_ids_by_county.setdefault(c, []).append(aid)

# COMMAND ----------

# ---- Sport types (with seasonality metadata) ----
sport_types = [
  ( 1, "cross-country skiing", True, "winter", "individual"),
  ( 2, "alpine skiing", True, "winter", "individual"),
  ( 3, "biathlon", True, "winter", "individual"),
  ( 4, "athletics", True, "summer", "individual"),
  ( 5, "football", True, "summer", "team"),
  ( 6, "cycling", True, "summer", "individual"),
  ( 7, "handball", False, "all_year", "team"),
  ( 8, "basketball", False, "all_year", "team"),
  ( 9, "volleyball", False, "all_year", "team"),
  (10, "swimming", False, "all_year", "individual"),
  (11, "orienteering", True, "summer", "individual"),
  (12, "ice hockey", False, "winter", "team"),
  (13, "ultra running", True, "summer", "individual"),
  (14, "taekwondo", False, "all_year", "individual"),
  (15, "soccer", True, "summer", "team"),
  (16, "tennis", False, "all_year", "individual"),
  (17, "squash", False, "all_year", "individual"),
  (18, "sailing", True, "summer", "team"),
  (19, "rugby", True, "summer", "team"),
  (20, "horse back riding", False, "all_year", "individual"),
]

# Popularity weight is used across the simulation to scale how many clubs/competitions/registrations
# a sport tends to get (1.0 = baseline).
SPORT_POPULARITY_WEIGHT = {
    "cross-country skiing": 3.0,
    "alpine skiing": 1.6,
    "biathlon": 1.3,
    "athletics": 1.2,
    "football": 3.2,
    "cycling": 1.0,
    "handball": 2.2,
    "basketball": 0.7,
    "volleyball": 0.8,
    "swimming": 1.4,
    "orienteering": 0.6,
    "ice hockey": 0.6,
    "ultra running": 0.5,
    "taekwondo": 0.4,
    "soccer": 3.2,
    "tennis": 0.6,
    "squash": 0.3,
    "sailing": 0.4,
    "rugby": 0.3,
    "horse back riding": 0.5,
}

def norm_sport_name(name: str) -> str:
    return (name or "").strip().lower()

sport_rows = [{
    "sport_type_id": sid,
    "sport_type_name": name,
    "is_outdoor": bool(outdoor),
    "season_peak": season,
    "sport_mode": mode,
    "popularity_weight": float(SPORT_POPULARITY_WEIGHT.get(norm_sport_name(name), 1.0))
} for sid, name, outdoor, season, mode in sport_types]

sport_df = spark.createDataFrame(sport_rows)
save_table(sport_df, "sport_types")

# ---- Helper: realistic timestamp on a given date (07:00–23:59) ----
def rand_ts(d, rnd=random) -> datetime:
    """Return a datetime on date d with a random time between 07:00 and 23:59."""
    return datetime.combine(d, datetime.min.time()) + timedelta(
        hours=rnd.randint(7, 23),
        minutes=rnd.randint(0, 59),
        seconds=rnd.randint(0, 59)
    )

# COMMAND ----------

# ---- Clubs (40% more) ----
club_types = ["grassroots","school","elite","company"]

clubs = []
for cid in range(1, N_CLUBS+1):
    # pick a municipality (weighted a little towards bigger cities)
    muni, county, _, _ = random.choice(municipalities + municipalities[:6]*2)
    addr_id = random.choice(addr_ids_by_muni[muni])
    ctype = random.choices(club_types, weights=[0.72,0.08,0.12,0.08])[0]
    created_at = date.today() - timedelta(days=random.randint(365, 365*15))
    clubs.append({
        "club_id": cid,
        "club_name": f"{muni} {random.choice(['IL','SK','IF','FK','HK','BK','SV'])} {cid}",
        "address_id": int(addr_id),
        "club_type": ctype,
        "website": f"https://{muni.lower().replace('ø','o').replace('å','a').replace('æ','ae')}-club{cid}.no",
        "created_at": created_at
    })

clubs_df = spark.createDataFrame(clubs)
save_table(clubs_df, "clubs")

club_geo = {r["club_id"]: addr_geo[r["address_id"]] for r in clubs_df.select("club_id","address_id").collect()}

# COMMAND ----------

# ---- club_sports mapping ----
sport_ids = [sid for sid, *_ in sport_types]

club_sports = []
# ---- club_sports mapping (POPULARITY-WEIGHTED) ----

# Build popularity lookup from sport_types table
sport_name_by_id = {r["sport_type_id"]: r["sport_type_name"]
                    for r in sport_df.select("sport_type_id","sport_type_name").collect()}

# Popularity multipliers (same logic as competitions)
SPORT_POPULARITY = {
    "cross-country skiing": 3.0,
    "football": 3.0,
    "soccer": 3.0,
    "handball": 2.6,
    "alpine skiing": 2.2,
    "swimming": 1.5,
    "athletics": 1.4,
    "cycling": 1.2,
    "volleyball": 1.1,
    "ice hockey": 1.2,
    "taekwondo": 0.7,
    "squash": 0.5,
    "rugby": 0.4,
}

def norm(name):
    return (name or "").strip().lower()

# Build weight per sport_id
sport_weight_by_id = {
    sid: SPORT_POPULARITY.get(norm(sport_name_by_id[sid]), 1.0)
    for sid in sport_ids
}

def weighted_sample_without_replacement(items, weights, k, rnd):
    items = list(items)
    weights = list(weights)
    chosen = []
    for _ in range(min(k, len(items))):
        total = sum(weights)
        r = rnd.random() * total
        s = 0.0
        for i, w in enumerate(weights):
            s += w
            if s >= r:
                chosen.append(items.pop(i))
                weights.pop(i)
                break
    return chosen

club_sports = []
for c in clubs:
    cid = c["club_id"]
    rnd = random.Random(SEED * 1234 + cid)

    # Most clubs have 1–4 sports
    k = rnd.choices([1,2,3,4], weights=[0.25,0.40,0.25,0.10])[0]

    picked = weighted_sample_without_replacement(
        sport_ids,
        [sport_weight_by_id[sid] for sid in sport_ids],
        k,
        rnd
    )

    for sid in picked:
        club_sports.append({
            "club_id": cid,
            "sport_type_id": sid
        })

club_sports_df = spark.createDataFrame(club_sports)
save_table(club_sports_df, "club_sports")

clubs_by_sport = {}
for r in club_sports_df.collect():
    clubs_by_sport.setdefault(r["sport_type_id"], []).append(r["club_id"])

# COMMAND ----------

# ---- Members (birth_date + created_at; email rules + nationality/migration) ----

# Faker-powered name pool to get:
# - lots of name variety (no more tiny hard-coded lists)
# - gender-consistent first names (female names → female gender, etc.)
# - realistic international mix for Norway
#
# Daily notebooks can sample from this table without requiring Faker installed.
NAME_POOL_SPECS = [
    # (country, faker_locale, rows_per_gender)
    ("Norway",         "no_NO", 2500),
    ("Sweden",         "sv_SE",  450),
    ("Denmark",        "da_DK",  350),
    ("Poland",         "pl_PL",  450),
    ("Lithuania",      "lt_LT",  300),
    ("Germany",        "de_DE",  250),
    ("United Kingdom", "en_GB",  250),
    ("France",         "fr_FR",  200),
    ("Spain",          "es_ES",  200),
]

def _safe_first_name(fk, gender: str) -> str:
    try:
        return fk.first_name_female() if gender == "female" else fk.first_name_male()
    except Exception:
        return fk.first_name()

name_rows = []
for country, locale, per_gender in NAME_POOL_SPECS:
    fk = Faker(locale)
    for g in ("female", "male"):
        for _ in range(int(per_gender)):
            name_rows.append({
                "country": country,
                "locale": locale,
                "gender": g,
                "first_name": _safe_first_name(fk, g),
                "last_name": fk.last_name()
            })

name_pool_df = spark.createDataFrame(name_rows)
save_table(name_pool_df, "name_pool")

# python-side lookup for deterministic sampling
pool_by_country_gender = {}
for r0 in name_rows:
    pool_by_country_gender.setdefault((r0["country"], r0["gender"]), []).append((r0["first_name"], r0["last_name"]))

# Nationality weights (tunable)
NATIONALITY_WEIGHTS = [
    ("Norway", 0.86),
    ("Sweden", 0.03),
    ("Denmark", 0.02),
    ("Poland", 0.03),
    ("Lithuania", 0.02),
    ("Germany", 0.01),
    ("United Kingdom", 0.01),
    ("France", 0.01),
    ("Spain", 0.01),
]
_n_countries = [c for c, _ in NATIONALITY_WEIGHTS]
_n_weights   = [w for _, w in NATIONALITY_WEIGHTS]

def pick_nationality(rnd) -> str:
    return rnd.choices(_n_countries, weights=_n_weights)[0]

def pick_country_of_birth(nationality: str, rnd) -> str:
    # Most members are born in Norway; some are born abroad (incl. Norwegian nationals born abroad).
    x = rnd.random()
    if x < 0.88:
        return "Norway"
    if nationality != "Norway" and x < 0.96:
        return nationality
    return rnd.choice([c for c in _n_countries if c != "Norway"])

def pick_moved_year(bdate: date, created_at: date, rnd) -> int:
    # Use membership created_at as a latest bound (the person must have moved before joining).
    if created_at.year <= bdate.year:
        return int(created_at.year)
    # More likely to move as a child than as a senior
    if rnd.random() < 0.60:
        max_y = min(created_at.year, bdate.year + rnd.randint(5, 18))
        return int(rnd.randint(bdate.year, max_y))
    # Move later in life
    min_y = max(bdate.year, created_at.year - rnd.randint(3, 25))
    return int(rnd.randint(min_y, created_at.year))

common_domains = ["gmail.com","hotmail.com","outlook.com","msn.com","yahoo.com"]
company_domains = ["knowit.no","fjordtech.no","nordiclabs.no","oslo-sport.no","bergen-sport.no","trondelag-idrett.no","arctic-sport.no"]
domains_weighted = common_domains*10 + company_domains*2

def clean_local(s: str) -> str:
    s = s.lower()
    s = (s.replace("æ","ae").replace("ø","o").replace("å","a"))
    s = "".join(ch for ch in s if ch.isalnum())
    return s

def gen_email(first, last, age_at_created, member_id, rnd):
    if age_at_created < 13:
        return None
    # 84.3% present (=> 15.7% missing)
    if rnd.random() > 0.843:
        return None
    # formats
    r = rnd.random()
    if r < 0.60:
        local = f"{clean_local(first)}.{clean_local(last)}"
    elif r < 0.95:
        local = f"{clean_local(first[0])}.{clean_local(last)}"
    else:
        # ~5% random-ish
        if rnd.random() < 0.6:
            local = f"{clean_local(first)}.{clean_local(last)}{rnd.randint(10,99)}"
        else:
            token = "".join(rnd.choices("abcdefghijklmnopqrstuvwxyz0123456789", k=4))
            local = f"{clean_local(first[:3])}{token}"
    return f"{local}@{rnd.choice(domains_weighted)}"

def sample_age(rnd):
    # roughly: many 16-40, some youth and seniors
    x = rnd.random()
    if x < 0.18:
        return rnd.randint(6, 15)
    if x < 0.78:
        # adult
        return int(max(16, min(66, rnd.gauss(29, 12))))
    return rnd.randint(67, 80)

def birth_date_from(age, created_at, rnd):
    year = created_at.year - int(age)
    month = rnd.randint(1, 12)
    day = rnd.randint(1, 28)  # safe day
    return date(year, month, day)

def pick_member_muni():
    # weight big cities higher
    base = municipalities + municipalities[:6]*3
    return random.choice(base)[0]

# map munis to clubs for locality-based primary club
clubs_by_muni = {}
clubs_by_county = {}
for cid, (m, c) in club_geo.items():
    clubs_by_muni.setdefault(m, []).append(cid)
    clubs_by_county.setdefault(c, []).append(cid)

def choose_primary_club(muni, county, rnd):
    if muni in clubs_by_muni and rnd.random() < 0.80:
        return rnd.choice(clubs_by_muni[muni])
    if county in clubs_by_county and rnd.random() < 0.85:
        return rnd.choice(clubs_by_county[county])
    return rnd.randint(1, N_CLUBS)

members_schema = StructType([
    StructField("member_id",           LongType(), False),
    StructField("first_name",          StringType(), False),
    StructField("last_name",           StringType(), False),
    StructField("gender",              StringType(), True),
    StructField("nationality",         StringType(), True),
    StructField("country_of_birth",    StringType(), True),
    StructField("moved_to_norway",     BooleanType(), False),
    StructField("moved_to_norway_year",IntegerType(), True),
    StructField("address_id",          LongType(), True),
    StructField("email",               StringType(), True),
    StructField("birth_date",          DateType(), True),
    StructField("created_at",          DateType(), True),
    StructField("phone",               StringType(), True),
    StructField("marketing_opt_in",    BooleanType(), True),
])

members = []
member_primary_club = {}
member_birth = {}

# 6 years of history for more realistic membership/affiliation timelines
members_start = date.today() - timedelta(days=365*6)

for mid in range(1, N_MEMBERS+1):
    rnd = random.Random(SEED*1_000_000 + mid)
    muni = pick_member_muni()
    addr_id = rnd.choice(addr_ids_by_muni[muni])
    muni_name, county_name = addr_geo[addr_id]
    cutoff = date.today() - timedelta(days=14)
    created_at = members_start + timedelta(days=rnd.randint(0, (cutoff-members_start).days))

    age = sample_age(rnd)
    bdate = birth_date_from(age, created_at, rnd)

    gender = rnd.choices(["female","male","non binary"], weights=[0.495,0.495,0.010])[0]
    name_gender = gender if gender in ("female","male") else rnd.choice(["female","male"])

    nationality = pick_nationality(rnd)

    # Sample name from pool; fallback to Norway if pool missing
    pool = pool_by_country_gender.get((nationality, name_gender)) or pool_by_country_gender.get(("Norway", name_gender))
    first, last = rnd.choice(pool)

    country_of_birth = pick_country_of_birth(nationality, rnd)
    moved = (country_of_birth != "Norway")
    moved_year = pick_moved_year(bdate, created_at, rnd) if moved else None

    email = gen_email(first, last, int(age), mid, rnd)
    phone = None if rnd.random() < 0.45 else f"+47{rnd.randint(90000000, 99999999)}"
    marketing_opt_in = True if (email is not None and rnd.random() < 0.62) else False

    members.append({
        "member_id": mid,
        "first_name": first,
        "last_name": last,
        "gender": gender,
        "nationality": nationality,
        "country_of_birth": country_of_birth,
        "moved_to_norway": bool(moved),
        "moved_to_norway_year": int(moved_year) if moved_year is not None else None,
        "address_id": int(addr_id),
        "email": email,
        "birth_date": bdate,
        "created_at": rand_ts(created_at, rnd),
        "phone": phone,
        "marketing_opt_in": bool(marketing_opt_in)
    })

    member_birth[mid] = bdate
    member_primary_club[mid] = choose_primary_club(muni_name, county_name, rnd)

members_df = spark.createDataFrame(members, schema=members_schema)
save_table(members_df, "members")

# COMMAND ----------

# ---- Affiliations (history; one current row per member) ----
aff_rows = []
for mid in range(1, N_MEMBERS+1):
    club_id = int(member_primary_club[mid])
    start = members[mid-1]["created_at"]
    aff_rows.append({
        "affiliation_id": int(mid),  # stable first affiliation
        "member_id": int(mid),
        "club_id": club_id,
        "start_date": start,
        "end_date": None,
        "reason": "new_member",
        "created_at": rand_ts(start, rnd),
        "updated_at": rand_ts(start, rnd)
    })

aff_df = spark.createDataFrame(aff_rows, schema=aff_schema)
save_table(aff_df, "affiliations")

# COMMAND ----------

# ---- Memberships + payments (history) ----
def age_on(d, b) -> int:
    d = d.date() if hasattr(d, "date") else d
    b = b.date() if hasattr(b, "date") else b
    return int((d - b).days // 365.25)

def membership_type_for_age(a: int, rnd):
    if a < 19:
        return "youth"
    if a < 67:
        return "adult"
    return "senior"

def price_for_type(t: str, rnd):
    base = {"trial": 0, "youth": 550, "adult": 1250, "senior": 850, "family": 2200}.get(t, 1200)
    # small variability
    return int(max(0, rnd.gauss(base, base*0.08)))

def membership_id(mid: int, start: date) -> int:
    return int(mid)*100_000_000 + int(start.strftime("%Y%m%d"))

memberships = []
payments = []

for m in members:
    mid = int(m["member_id"])
    created_at = m["created_at"]
    bdate = m["birth_date"]
    rnd = random.Random(SEED*2_000_000 + mid)

    # Some start with trial
    first_type = "trial" if (age_on(created_at, bdate) >= 13 and rnd.random() < 0.08) else membership_type_for_age(age_on(created_at, bdate), rnd)
    # Small share family
    if first_type in ("adult","senior") and rnd.random() < 0.05:
        first_type = "family"

    cur_start = created_at.date() if hasattr(created_at, "date") else created_at
    # simulate renewals year-by-year with churn
    for period in range(1, 8):  # up to ~8 years history max, but most will stop earlier
        mtype = first_type if period == 1 else membership_type_for_age(age_on(cur_start, bdate), rnd)
        if period > 1 and rnd.random() < 0.05:
            mtype = "family"

        duration_days = 30 if mtype == "trial" else 365
        end_date = (cur_start + timedelta(days=duration_days))
        end_date = end_date.date() if hasattr(end_date, "date") else end_date

        status = "active" if end_date >= date.today() else "expired"
        mid_id = membership_id(mid, cur_start)
        price = price_for_type(mtype, rnd)

        memberships.append({
            "membership_id": mid_id,
            "member_id": mid,
            "membership_type": mtype,
            "status": status,
            "start_date": cur_start,
            "end_date": end_date,
            "price_nok": int(price),
            "created_at": rand_ts(cur_start, rnd),
            "updated_at": rand_ts(cur_start, rnd)
        })

        # payment for paid memberships (trials may be free)
        if price > 0:
            method = rnd.choices(["vipps","card","invoice"], weights=[0.45,0.35,0.20])[0]
            # invoice can be pending
            if method == "invoice" and rnd.random() < 0.25:
                p_status = "pending"
                paid_at = None
            else:
                p_status = rnd.choices(["paid","failed","refunded"], weights=[0.965,0.025,0.010])[0]
                paid_at = rand_ts(cur_start, rnd) + timedelta(days=rnd.randint(0, 10), hours=rnd.randint(0,23), minutes=rnd.randint(0,59)) if p_status == "paid" else None

            payments.append({
                "payment_id": mid_id*10 + 1,
                "membership_id": mid_id,
                "member_id": mid,
                "amount_nok": int(price),
                "currency": "NOK",
                "method": method,
                "status": p_status,
                "attempt": 1,
                "created_at": rand_ts(cur_start, rnd),
                "paid_at": paid_at,
                "updated_at": rand_ts(cur_start, rnd)
            })

        # stop if beyond today or churned
        if end_date >= date.today():
            break

        # churn probability
        renew_prob = 0.82 if mtype in ("adult","youth") else 0.75
        if rnd.random() > renew_prob:
            break

        # next start
        cur_start = end_date

memberships_df = spark.createDataFrame(memberships)
save_table(memberships_df, "memberships")

payments_df = spark.createDataFrame(payments)
save_table(payments_df, "membership_payments")

# COMMAND ----------

# ---- Competitions (weekend-heavy + seasonality + capacity) ----
def weighted_choice(items, weights, rnd):
    total = sum(weights)
    r = rnd.random() * total
    upto = 0.0
    for item, w in zip(items, weights):
        upto += w
        if upto >= r:
            return item
    return items[-1]

# --- Sport popularity multipliers (tune as you like) ---
# Bigger number => more competitions (and therefore more participation+results rows) for that sport.
SPORT_POPULARITY = {
    "cross-country skiing": 2.8,
    "alpine skiing":        1.6,
    "biathlon":             1.3,

    # Most popular
    "football":             3.2,
    "soccer":               3.2,  # treat as alias if you keep both names
    "handball":             2.2,

    # Medium
    "swimming":             1.4,
    "athletics":            1.2,
    "cycling":              1.0,

    # Lower (still present)
    "basketball":           0.7,
    "volleyball":           0.8,
    "ice hockey":           0.6,
    "orienteering":         0.6,

    # “Long tail” sports
    "ultra running":        0.5,
    "taekwondo":            0.4,
    "tennis":               0.6,
    "squash":               0.3,
    "sailing":              0.4,
    "rugby":                0.3,
    "horse back riding":    0.5,
}

def _norm_sport(name: str) -> str:
    return (name or "").strip().lower()

def sport_weight_for_month(sport_row, month: int) -> float:
    """
    Combined weight = popularity * seasonality * outdoor adjustments
    """
    # 1) Popularity (base, all year)
    w = SPORT_POPULARITY.get(_norm_sport(sport_row["sport_type_name"]), 1.0)

    # 2) Seasonality
    winter = month in (11, 12, 1, 2, 3)
    summer = month in (5, 6, 7, 8, 9)
    peak = sport_row["season_peak"]
    out = bool(sport_row["is_outdoor"])

    if peak == "winter":
        w *= 3.2 if winter else 0.55
    elif peak == "summer":
        w *= 3.0 if summer else 0.65
    else:
        w *= 1.15

    # 3) Outdoor non-winter sports slightly less in deep winter
    if out and winter and peak != "winter":
        w *= 0.8

    return w
    
sport_meta = sport_df.select("sport_type_id","sport_type_name","is_outdoor","season_peak","sport_mode").collect()
sport_meta_rows = [r.asDict() for r in sport_meta]

levels = ["Local","Regional","National"]

# ~1 year more history than before: start ~4 years ago, range ~4.5 years (includes ~6 months future)
CUTOFF_DATE = date.today() - timedelta(days=14)
start_base = CUTOFF_DATE.replace(month=1, day=1) - timedelta(days=365*4)
range_days = (CUTOFF_DATE - start_base).days

def pick_start_date(rnd):
    # weekend-weighted dates within range
    base = start_base + timedelta(days=rnd.randint(0, range_days))
    # shift slightly towards weekends
    for _ in range(4):
        if base.weekday() in (5,6):  # Sat/Sun
            break
        if rnd.random() < 0.55:
            base += timedelta(days=1)
    return base

def competition_capacity(level: str, rnd, popular: bool):
    if level == "Local":
        base = rnd.randint(40, 120)
    elif level == "Regional":
        base = rnd.randint(120, 260)
    else:
        base = rnd.randint(200, 450)
    # popular events may have tight capacity (more waitlist)
    if popular:
        base = int(base * rnd.uniform(0.65, 0.90))
    return max(20, base)

competitions = []
comp_popular = {}
for cid in range(1, N_COMPETITIONS+1):
    rnd = random.Random(SEED*3_000_000 + cid)
    start = pick_start_date(rnd)
    dur = rnd.choices([1,2,3], weights=[0.55,0.30,0.15])[0]
    end = start + timedelta(days=dur-1)

    level = rnd.choices(levels, weights=[0.55,0.30,0.15])[0]

    # sport selection based on start month
    weights = [sport_weight_for_month(s, start.month) for s in sport_meta_rows]
    srow = weighted_choice(sport_meta_rows, weights, rnd)
    sport_id = int(srow["sport_type_id"])

    # host club must offer sport
    host_candidates = clubs_by_sport.get(sport_id, list(range(1, N_CLUBS+1)))
    host_club_id = int(rnd.choice(host_candidates))

    host_muni, host_county = club_geo[host_club_id]
    # competition address near host
    if host_muni in addr_ids_by_muni and rnd.random() < 0.80:
        addr_id = int(rnd.choice(addr_ids_by_muni[host_muni]))
    else:
        addr_id = int(rnd.choice(addr_ids_by_county.get(host_county, list(addr_geo.keys()))))

    # created_at and registration deadline
    lead = rnd.randint(7, 120) if level != "Local" else rnd.randint(3, 60)
    created_at = max(start_base, start - timedelta(days=lead))
    reg_deadline = start - timedelta(days=rnd.randint(1, 21))
    if reg_deadline < created_at:
        reg_deadline = created_at

    popular = (rnd.random() < 0.22)
    cap = competition_capacity(level, rnd, popular)
    comp_popular[cid] = popular

    # status
    if end < date.today():
        status = "cancelled" if rnd.random() < 0.03 else "completed"
    else:
        status = "cancelled" if rnd.random() < 0.01 else "scheduled"

    competitions.append({
        "competition_id": cid,
        "name": f"{host_muni} {srow['sport_type_name'].title()} {start.year} #{cid}",
        "sport_type_id": sport_id,
        "host_club_id": host_club_id,
        "address_id": addr_id,
        "venue": rnd.choice(["Idrettshall","Stadion","Friidrettsbane","Skisenter","Svømmehall","Arena"]),
        "level": level,
        "start_date": start,
        "end_date": end,
        "status": status,
        "created_at": rand_ts(created_at, rnd),
        "registration_deadline": reg_deadline,
        "capacity": int(cap),
        "updated_at": rand_ts(created_at, rnd)
    })

competitions_df = spark.createDataFrame(competitions)
save_table(competitions_df, "competitions")

# competition geo lookup
comp_geo = {r["competition_id"]: addr_geo[r["address_id"]] for r in competitions_df.select("competition_id","address_id").collect()}
sport_by_comp = {r["competition_id"]: r["sport_type_id"] for r in competitions_df.select("competition_id","sport_type_id").collect()}

# COMMAND ----------

# ---- Participation + waitlist (capacity-aware) ----
# We create registrations for a subset of members, with locality bias.
# If over capacity, overflow goes to competition_waitlist.

def participation_weight(age: int) -> float:
    # more active in teens/young adults
    if age < 13: return 0.25
    if age < 19: return 1.25
    if age < 36: return 1.10
    if age < 56: return 0.90
    return 0.65

# members geo cached
mem_geo = {r["member_id"]: addr_geo[r["address_id"]] for r in members_df.select("member_id","address_id").collect()}

# active membership marker
active_members = set([r["member_id"] for r in memberships_df.where(F.col("status") == F.lit("active")).select("member_id").distinct().collect()])

participation_rows = []
waitlist_rows = []
pid = 1
wid = 1

for c in competitions:
    cid = int(c["competition_id"])
    start = c["start_date"]
    level = c["level"]
    cap = int(c["capacity"])
    status = c["status"]

    # skip cancelled
    if status == "cancelled":
        continue

    # expected participants by level
    base_n = {"Local": 45, "Regional": 80, "National": 140}[level]
    rnd = random.Random(SEED*4_000_000 + cid)

    # future comps get fewer registrations (early funnel)
    days_to_start = (start - date.today()).days
    funnel = 0.25 if days_to_start > 30 else (0.55 if days_to_start > 7 else 0.85)
    target_n = max(10, int(rnd.gauss(base_n, base_n*0.30) * funnel))

    # popular events slightly oversubscribe to create waitlist
    if comp_popular.get(cid, False):
        target_n = int(target_n * rnd.uniform(1.10, 1.60))

    comp_muni, comp_county = addr_geo[c["address_id"]]

    # Build a candidate pool biased to same muni/county and active members
    # (simple: random sample then filter)
    candidates = []
    # sample ~3x target to allow filtering
    sample_size = min(N_MEMBERS, max(2000, target_n*30))
    for mid in rnd.sample(range(1, N_MEMBERS+1), k=sample_size):
        if mid not in active_members:
            continue
        muni, county = mem_geo[mid]
        w = participation_weight(age_on(start, member_birth[mid]))
        if muni == comp_muni:
            w *= 1.35
        elif county == comp_county:
            w *= 1.12
        # small chance drop
        if rnd.random() < min(0.98, w/2.5):
            candidates.append(mid)

    # ensure enough
    if len(candidates) < target_n:
        # pad with random actives
        extra = [m for m in rnd.sample(list(active_members), k=min(len(active_members), target_n*2)) if m not in candidates]
        candidates.extend(extra)

    candidates = candidates[:target_n]

    # Assign to participation up to cap, overflow to waitlist
    # club_id from primary affiliation map
    for i, mid in enumerate(candidates, start=1):
        club_id = int(member_primary_club[mid])
        # registered_at: 7–60 days before start for past, or 0–30 for near-future
        if start < date.today():
            reg_day = start - timedelta(days=rnd.randint(0, 60))
        else:
            reg_day = max(c["created_at"], start - timedelta(days=rnd.randint(1, 30)))
        reg_ts = rand_ts(reg_day, rnd)

        if i <= cap:
            # status distribution: future mostly registered; past more checked_in/dns/cancelled
            if start > date.today():
                st = rnd.choices(["registered","cancelled"], weights=[0.96,0.04])[0]
            else:
                st = rnd.choices(["checked_in","dns","cancelled"], weights=[0.85,0.10,0.05])[0]

            participation_rows.append({
                "participation_id": pid,
                "competition_id": cid,
                "member_id": mid,
                "club_id": club_id,
                "registered_at": reg_ts,
                "status": st,
                "status_updated_at": reg_ts,
                "source": rnd.choices(["web","mobile","club_admin"], weights=[0.55,0.35,0.10])[0],
                "bib_number": None
            })
            pid += 1
        else:
            waitlist_rows.append({
                "waitlist_id": wid,
                "competition_id": cid,
                "member_id": mid,
                "club_id": club_id,
                "added_at": reg_ts,
                "status": "waiting",
                "promoted_participation_id": None,
                "updated_at": reg_ts
            })
            wid += 1

participation_df = spark.createDataFrame(participation_rows, schema=part_schema)
save_table(participation_df, "participation")

waitlist_df = spark.createDataFrame(waitlist_rows, schema=wait_schema)
save_table(waitlist_df, "competition_waitlist")

# COMMAND ----------

# ---- Results (for completed competitions) ----
def sport_result(sport_name: str, rnd):
    if sport_name in ["cross-country skiing","alpine skiing","athletics","cycling","orienteering","biathlon"]:
        base = rnd.uniform(600, 7200)  # 10 min to 2 hours
        noise = rnd.uniform(-base*0.12, base*0.12)
        t = max(60, int(base + noise))
        return None, t, None
    # team/points
    score = rnd.randint(0, 100) if sport_name=="basketball" else rnd.randint(0, 15)
    return score, None, None

sport_name_by_id = {r["sport_type_id"]: r["sport_type_name"] for r in sport_df.select("sport_type_id","sport_type_name").collect()}
sport_name_by_comp = {c["competition_id"]: sport_name_by_id[c["sport_type_id"]] for c in competitions}

# map comp end date for official flag
comp_end = {c["competition_id"]: c["end_date"] for c in competitions}

# group participation by competition
p_rows = participation_df.collect()
by_comp = {}
for r in p_rows:
    if r["status"] in ("checked_in","registered") and comp_end[r["competition_id"]] < date.today():
        by_comp.setdefault(r["competition_id"], []).append(r)

results = []
for comp_id, parts in by_comp.items():
    rnd = random.Random(SEED*5_000_000 + int(comp_id))
    sname = sport_name_by_comp[int(comp_id)]
    # take ~85% starters (exclude cancelled)
    starters = [p for p in parts if p["status"] != "cancelled" and rnd.random() < 0.85]
    if not starters:
        continue

    rows = []
    for p in starters:
        score, tsec, _ = sport_result(sname, rnd)
        note = None if rnd.random() < 0.93 else rnd.choice(["photo finish","wind aided","course short","equipment issue","injury"])
        rows.append({
            "participation_id": int(p["participation_id"]),
            "score": int(score) if score is not None else None,
            "time_seconds": int(tsec) if tsec is not None else None,
            "notes": note
        })

    # rank
    if sname in ["cross-country skiing","alpine skiing","athletics","cycling","orienteering","biathlon"]:
        rows.sort(key=lambda x: x["time_seconds"] if x["time_seconds"] is not None else 9_999_999)
    else:
        rows.sort(key=lambda x: -(x["score"] if x["score"] is not None else -1))

    end_date = comp_end[int(comp_id)]
    is_official = (date.today() - end_date).days > 7
    recorded_at = rand_ts(end_date, rnd) + timedelta(hours=rnd.randint(12, 22))

    for pos, r in enumerate(rows, start=1):
        results.append({
            "result_id": int(r["participation_id"]),   # deterministic
            "participation_id": int(r["participation_id"]),
            "position": int(pos),
            "score": r["score"],
            "time_seconds": r["time_seconds"],
            "notes": r["notes"],
            "is_official": bool(is_official),
            "recorded_at": recorded_at,
            "corrected_at": None
        })
results_df = spark.createDataFrame(results, schema=results_schema)
save_table(results_df, "results")

# COMMAND ----------

print("Done. Tables in", f"{TARGET_CATALOG}.{TARGET_SCHEMA}")
spark.sql(f"SHOW TABLES IN `{TARGET_CATALOG}`.`{TARGET_SCHEMA}`").show(50, truncate=False)

# COMMAND ----------

# ---- Export all tables to landing volume as parquet files ----
# Bootstrap date: use today so Auto Loader picks up as initial load
from datetime import date
BOOTSTRAP_DATE = date.today().isoformat()
LANDING_ROOT   = "/Volumes/sport_lakehouse/landing/sports"

TABLES_TO_EXPORT = [
    ("addresses",           addresses_df),
    ("sport_types",         sport_df),
    ("clubs",               clubs_df),
    ("club_sports",         club_sports_df),
    ("members",             members_df),
    ("affiliations",        aff_df),
    ("memberships",         memberships_df),
    ("membership_payments", payments_df),
    ("competitions",        competitions_df),
    ("participation",       participation_df),
    ("competition_waitlist",waitlist_df),
    ("results",             results_df),
]

print(f"\nExporting {len(TABLES_TO_EXPORT)} tables to {LANDING_ROOT}/<table>/{BOOTSTRAP_DATE}/")

for table_name, df in TABLES_TO_EXPORT:
    path = f"{LANDING_ROOT}/{table_name}/{BOOTSTRAP_DATE}/"
    df.write.mode("overwrite").parquet(path)
    print(f"  ✅ {table_name}: {df.count()} rows → {path}")

# club_weather_monitoring and weather_stations are not in the initial export
# They are generated by 18_club_weather_daily and weather_stations_daily
print("\n✅ Bootstrap export to landing volume complete.")