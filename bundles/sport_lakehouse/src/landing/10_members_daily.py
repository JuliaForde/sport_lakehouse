# Databricks notebook source
# 10_members_daily — add new members daily
# - birth_date (age can be calculated downstream)
# - email rules (under 13 never have email; otherwise 84.3% chance)
# - nationality + country_of_birth + moved_to_norway fields
# - gender-consistent names by sampling from bootstrap-generated name_pool
# Writes: members

# COMMAND ----------

from datetime import date

dbutils.widgets.text("run_date",date.today().isoformat() )          # <- Monday
dbutils.widgets.text("volume", "medium")                # low|medium|high
dbutils.widgets.text("volume_factor", "1.0")            # scales volume_mult

# COMMAND ----------

# MAGIC %run ./00_utils

# COMMAND ----------

# Deterministic seed for this daily run (used for Spark rand seeds below)
SEED = seed_for("members_daily_seed", run_date=RUN_DATE)


# COMMAND ----------

import random
from datetime import date, timedelta
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, LongType, IntegerType, StringType, BooleanType, DateType

TABLE = "members"
ID_COL = "member_id"

rnd = random.Random(seed_for(TABLE))

# base volume (scaled by VOLUME_MULT)
base_min, base_max = 30, 120
min_n = max(1, int(base_min * VOLUME_MULT))
max_n = max(min_n, int(base_max * VOLUME_MULT))
proposed_n = rnd.randint(min_n, max_n)

alloc = allocate_ids(TABLE, ID_COL, proposed_n)
start_id, n_new, already = alloc["start_id"], alloc["n_rows"], alloc["already_ran"]

if n_new == 0:
    print("No new members today (n_new=0).")
else:
    # Address lookups
    addr = spark.table(tbl("addresses")).select("address_id","municipality_name","county_name")
    addr_muni = [r["municipality_name"] for r in addr.select("municipality_name").distinct().collect()]
    addr_ids_by_muni = {r["municipality_name"]: r["ids"]
                        for r in addr.groupBy("municipality_name").agg(F.collect_list("address_id").alias("ids")).collect()}

    # Name pool (generated in bootstrap)
    try:
        np = spark.table(tbl("name_pool")).select("country","gender","first_name","last_name").collect()
    except Exception as e:
        raise Exception("Missing table name_pool. Run 00_bootstrap_generate_sports_dataset first to create it.") from e

    pool_by_country_gender = {}
    for x in np:
        pool_by_country_gender.setdefault((x["country"], x["gender"]), []).append((x["first_name"], x["last_name"]))

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
    countries = [c for c, _ in NATIONALITY_WEIGHTS]
    weights   = [w for _, w in NATIONALITY_WEIGHTS]

    def pick_nationality(r):
        return r.choices(countries, weights=weights)[0]

    def pick_country_of_birth(nationality, r):
        # Most members are born in Norway; some are born abroad (incl. Norwegian nationals born abroad).
        x = r.random()
        if x < 0.88:
            return "Norway"
        if nationality != "Norway" and x < 0.96:
            return nationality
        return r.choice([c for c in countries if c != "Norway"])

    def pick_moved_year(bdate: date, created_at: date, r) -> int:
        if created_at.year <= bdate.year:
            return int(created_at.year)
        if r.random() < 0.60:
            max_y = min(created_at.year, bdate.year + r.randint(5, 18))
            return int(r.randint(bdate.year, max_y))
        min_y = max(bdate.year, created_at.year - r.randint(3, 25))
        return int(r.randint(min_y, created_at.year))

    # Email configuration
    common_domains = ["gmail.com","hotmail.com","outlook.com","msn.com","yahoo.com"]
    company_domains = ["knowit.no","fjordtech.no","nordiclabs.no","oslo-sport.no","bergen-sport.no","trondelag-idrett.no","arctic-sport.no", "wilhelmsen.no", "rema.no", "plantasjen.no", "qlworks.no",  "nordic.net","ework.com", "hjort.no", "accanto.com","belu.no", "coop.no","kiwi.no", "oslokom.no","nif.no"]
    domains_weighted = common_domains*10 + company_domains*2

    def clean_local(s: str) -> str:
        s = s.lower().replace("æ","ae").replace("ø","o").replace("å","a")
        return "".join(ch for ch in s if ch.isalnum())

    def sample_age(r):
        x = r.random()
        if x < 0.18: return r.randint(6, 15)
        if x < 0.78: return int(max(16, min(66, r.gauss(29, 12))))
        return r.randint(67, 80)

    def birth_date_from(age, created_at, r):
        year = created_at.year - int(age)
        month = r.randint(1, 12)
        day = r.randint(1, 28)
        return date(year, month, day)

    def gen_email(first, last, age, member_id, r):
        if age < 13:
            return None
        # 84.3% present
        if r.random() > 0.843:
            return None
        rr = r.random()
        if rr < 0.60:
            local = f"{clean_local(first)}.{clean_local(last)}"
        elif rr < 0.95:
            local = f"{clean_local(first[0])}.{clean_local(last)}"
        else:
            if r.random() < 0.6:
                local = f"{clean_local(first)}.{clean_local(last)}{r.randint(10,99)}"
            else:
                token = "".join(r.choices("abcdefghijklmnopqrstuvwxyz0123456789", k=4))
                local = f"{clean_local(first[:3])}{token}"
        return f"{local}@{r.choice(domains_weighted)}"

    members_schema = StructType([
        StructField("member_id",            LongType(), False),
        StructField("first_name",           StringType(), False),
        StructField("last_name",            StringType(), False),
        StructField("gender",               StringType(), True),
        StructField("nationality",          StringType(), True),
        StructField("country_of_birth",     StringType(), True),
        StructField("moved_to_norway",      BooleanType(), False),
        StructField("moved_to_norway_year", IntegerType(), True),
        StructField("address_id",           LongType(), True),
        StructField("email",                StringType(), True),
        StructField("birth_date",           DateType(), True),
        StructField("created_at",           DateType(), True),
        StructField("phone",                StringType(), True),
        StructField("marketing_opt_in",     BooleanType(), True),
    ])

    rows=[]
    for i in range(n_new):
        member_id = start_id + i + 1
        r = random.Random(seed_for(TABLE) + int(member_id))

        muni = r.choice(addr_muni)
        addr_id = int(r.choice(addr_ids_by_muni[muni]))

        created_at = RUN_DATE
        age = sample_age(r)
        bdate = birth_date_from(age, created_at, r)

        gender = r.choices(["female","male","non binary"], weights=[0.41,0.58,0.01])[0]
        name_gender = gender if gender in ("female","male") else r.choice(["female","male"])

        nationality = pick_nationality(r)
        pool = pool_by_country_gender.get((nationality, name_gender)) or pool_by_country_gender.get(("Norway", name_gender))
        first, last = r.choice(pool)

        country_of_birth = pick_country_of_birth(nationality, r)
        moved = (country_of_birth != "Norway")
        moved_year = pick_moved_year(bdate, created_at, r) if moved else None

        email = gen_email(first, last, age, member_id, r)
        phone = None if r.random() < 0.45 else f"+47{r.randint(90000000, 99999999)}"
        marketing_opt_in = True if (email is not None and r.random() < 0.62) else False

        rows.append({
            "member_id": int(member_id),
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
            "created_at": created_at,
            "phone": phone,
            "marketing_opt_in": bool(marketing_opt_in)
        })

    df_new = spark.createDataFrame(rows, schema=members_schema)
    merge_into(TABLE, df_new, ["member_id"])
# -------------------------
# ALSO simulate member moves (address changes) for existing members
# 80% within same municipality, 20% to random municipality
# -------------------------
MOVE_WITHIN_RATE = 0.80
MOVE_RATE_BY_VOLUME = {"low": 0.0003, "medium": 0.0008, "high": 0.0015}  # fraction of members/day
move_rate = MOVE_RATE_BY_VOLUME.get(VOLUME, 0.0008) * VOLUME_MULT

# Only move members that existed BEFORE today's insert (avoid "move on day 1")
members_existing = (spark.table(tbl("members"))
    .where(F.col("member_id") <= F.lit(start_id))   # start_id is last allocated id before today's new members
    .select("member_id","address_id")
)

# Join to current municipality
m = (members_existing
     .join(addr.selectExpr("address_id as addr_id_cur", "municipality_name as muni_cur"),
           F.col("address_id") == F.col("addr_id_cur"), "left")
     .select("member_id", F.col("address_id").alias("address_id_cur"), "muni_cur")
)

# Pick movers stochastically (0..many per day)
movers = (m
  .withColumn("u", F.rand(seed=SEED + int(RUN_DATE.strftime("%Y%m%d"))))
  .where(F.col("u") < F.lit(move_rate))
  .drop("u")
)

# Decide within vs cross municipality
movers = movers.withColumn(
    "move_kind",
    F.when(F.rand(seed=SEED + 77) < F.lit(MOVE_WITHIN_RATE), F.lit("within"))
     .otherwise(F.lit("cross"))
)

# We already built addr_ids_by_muni earlier:
# addr_ids_by_muni = { muni: [address_id,...] }

munis = list(addr_ids_by_muni.keys())

import random
def pick_new_address(mid, muni_cur, move_kind, cur_addr):
    r = random.Random(seed_for("member_move") + int(mid))
    if move_kind == "within":
        cands = [int(x) for x in addr_ids_by_muni.get(muni_cur, []) if int(x) != int(cur_addr)]
        if not cands:
            return None
        return int(r.choice(cands))
    else:
        if len(munis) <= 1:
            return None
        muni_new = muni_cur
        for _ in range(6):
            muni_new = r.choice(munis)
            if muni_new != muni_cur:
                break
        cands = [int(x) for x in addr_ids_by_muni.get(muni_new, [])]
        return int(r.choice(cands)) if cands else None

pick_addr_udf = F.udf(pick_new_address, "bigint")

moves = (movers
  .withColumn("new_address_id", pick_addr_udf("member_id","muni_cur","move_kind","address_id_cur"))
  .where(F.col("new_address_id").isNotNull())
  .select("member_id", F.col("new_address_id").alias("address_id"))
)

# Important: your merge expects full member rows (because merge_into will update columns).
# So we build "full rows" from current members and only replace address_id.
members_df = spark.table(tbl("members")).alias("mem")
moves_df   = moves.alias("mv")

# Build list of member columns but exclude the original address_id
mem_cols = [f.name for f in members_schema.fields]
mem_cols_no_addr = [c for c in mem_cols if c != "address_id"]

# Join and use the address_id coming from moves (mv.address_id)
df_moves_full = (
    members_df
    .join(moves_df, F.col("mem.member_id") == F.col("mv.member_id"), "inner")
    .select(
        *[F.col(f"mem.{c}").alias(c) for c in mem_cols_no_addr],   # original member columns except address_id
        F.col("mv.address_id").alias("address_id")                # new address_id from moves
    )
)

# Merge updates (address changes)
if df_moves_full.count() > 0:
    merge_into(TABLE, df_moves_full.select([f.name for f in members_schema.fields]), ["member_id"])
    print(f"Moved members updated: {df_moves_full.count()}")
else:
    print("No member moves today.")
    if not already:
        record_state(TABLE, start_id, start_id+n_new, n_new, notes=f"Inserted {n_new} members")

    display(df_new.limit(20))

# COMMAND ----------

# Combine today's inserts and updates into one batch, then export via the
# shared helper (overwrite per run_date partition — idempotent).
dfs_to_export = []
if 'df_new' in locals() and n_new > 0:
    dfs_to_export.append(df_new)
if 'df_moves_full' in locals() and df_moves_full.count() > 0:
    dfs_to_export.append(df_moves_full)

if not dfs_to_export:
    export_to_landing(TABLE, None)
else:
    df_export = dfs_to_export[0]
    for df in dfs_to_export[1:]:
        df_export = df_export.unionByName(df)
    export_to_landing(TABLE, df_export)