# Databricks notebook source
# 20_competitions_daily — add new competitions with:
# - weekday may be 0 competitions
# - weekend-heavy starts (but not all)
# - sport seasonality (winter vs summer)
# Writes: competitions

# COMMAND ----------

from datetime import date

dbutils.widgets.removeAll()
dbutils.widgets.text("run_date",date.today().isoformat() )          # <- Monday
dbutils.widgets.text("volume", "medium")                # low|medium|high
dbutils.widgets.text("volume_factor", "1.0")            # scales volume_mult

# COMMAND ----------

# MAGIC %run ./00_utils

# COMMAND ----------

# DBTITLE 1,Cell 4
import random
from datetime import timedelta, datetime
from pyspark.sql import functions as F

TABLE = "competitions"
ID_COL = "competition_id"
r = random.Random(seed_for(TABLE))
df_new_competitions = None  # Initialize at notebook level

# Day-of-week probability of creating competitions today
dow = RUN_DATE.weekday()  # Mon=0 ... Sun=6
base_p = {0:0.25, 1:0.25, 2:0.25, 3:0.25, 4:0.55, 5:0.92, 6:0.80}[dow]
# Adjust by volume (high => slightly higher chance; low => lower)
p_has = min(0.98, max(0.05, base_p * (0.85 + 0.25*VOLUME_MULT)))

if r.random() > p_has:
    proposed_n = 0
else:
    # range by weekday vs weekend
    if dow in (0,1,2,3):      # Mon–Thu
        lo, hi = 1, 3
    elif dow == 4:            # Fri
        lo, hi = 1, 5
    elif dow == 5:            # Sat
        lo, hi = 2, 10
    else:                     # Sun
        lo, hi = 1, 7
    lo = max(0, int(lo * VOLUME_MULT))
    hi = max(lo, int(hi * VOLUME_MULT))
    proposed_n = r.randint(lo, hi)

alloc = allocate_ids(TABLE, ID_COL, proposed_n)
start_id, n_new, already = alloc["start_id"], alloc["n_rows"], alloc["already_ran"]

if n_new == 0:
    print(f"No competitions generated today (p_has={p_has:.2f}).")
else:
    sport = spark.table(tbl("sport_types")).select("sport_type_id","season_peak","is_outdoor","sport_type_name","popularity_weight","sport_mode").collect()
    sport_rows = [x.asDict() for x in sport]

    club_sports = spark.table(tbl("club_sports")).select("club_id","sport_type_id")
    clubs_by_sport = {r["sport_type_id"]: r["club_ids"]
                      for r in club_sports.groupBy("sport_type_id").agg(F.collect_list("club_id").alias("club_ids")).collect()}

    clubs = spark.table(tbl("clubs")).select("club_id","address_id")
    addr = spark.table(tbl("addresses")).select("address_id","municipality_name","county_name")
    club_geo = (clubs.join(addr, on="address_id", how="left")
                    .select("club_id","address_id","municipality_name","county_name")
                    .collect())
    club_geo_map = {int(x["club_id"]): (int(x["address_id"]), x["municipality_name"], x["county_name"]) for x in club_geo}

    addr_ids_by_muni = {r["municipality_name"]: r["ids"]
                        for r in addr.groupBy("municipality_name").agg(F.collect_list("address_id").alias("ids")).collect()}
    addr_ids_by_county = {r["county_name"]: r["ids"]
                          for r in addr.groupBy("county_name").agg(F.collect_list("address_id").alias("ids")).collect()}

    all_address_ids = [int(x['address_id']) for x in addr.select('address_id').collect()]

    def sport_weight(s, month):
        winter = month in (11,12,1,2,3)
        summer = month in (5,6,7,8,9)
        peak = s["season_peak"]
        out = bool(s["is_outdoor"])
        w = 1.0
        if peak == "winter":
            w *= 3.2 if winter else 0.55
        elif peak == "summer":
            w *= 3.0 if summer else 0.65
        else:
            w *= 1.15
        if out and winter and peak != "winter":
            w *= 0.8
        # popularity: more competitions for popular sports
        w *= float(s.get("popularity_weight", 1.0))
        return w

    def weighted_choice(items, weights, rr):
        tot = sum(weights)
        x = rr.random() * tot
        s = 0.0
        for it, w in zip(items, weights):
            s += w
            if s >= x:
                return it
        return items[-1]

    def pick_start_date(rr):
        # pick within next 45 days, weekend-biased
        base = RUN_DATE + timedelta(days=rr.randint(0, 45))
        # nudge towards weekend
        for _ in range(3):
            if base.weekday() in (5,6):
                break
            if rr.random() < 0.60:
                base += timedelta(days=1)
        return base

    def capacity(level, rr, popular):
        if level == "Local":
            base = rr.randint(40, 120)
        elif level == "Regional":
            base = rr.randint(120, 260)
        else:
            base = rr.randint(200, 450)
        if popular:
            base = int(base * rr.uniform(0.65, 0.90))
        return max(20, base)

    levels = ["Local","Regional","National"]
    venues = ["Idrettshall","Stadion","Friidrettsbane","Skisenter","Svømmehall","Arena"]

    rows=[]
    for i in range(n_new):
        comp_id = start_id + i + 1
        rr = random.Random(seed_for(TABLE) + int(comp_id))
        start = pick_start_date(rr)
        dur = rr.choices([1,2,3], weights=[0.55,0.30,0.15])[0]
        end = start + timedelta(days=dur-1)

        level = rr.choices(levels, weights=[0.55,0.30,0.15])[0]
        weights = [sport_weight(s, start.month) for s in sport_rows]
        srow = weighted_choice(sport_rows, weights, rr)
        sport_id = int(srow["sport_type_id"])

        host_candidates = clubs_by_sport.get(sport_id, list(club_geo_map.keys()))
        host_club_id = int(rr.choice(host_candidates))
        host_addr_id, host_muni, host_county = club_geo_map[host_club_id]

        # competition address near host
        if host_muni in addr_ids_by_muni and rr.random() < 0.80:
            address_id = int(rr.choice(addr_ids_by_muni[host_muni]))
        else:
            address_id = int(rr.choice(addr_ids_by_county.get(host_county, all_address_ids)))

        created_at = RUN_DATE
        reg_deadline = max(RUN_DATE, start - timedelta(days=rr.randint(1,21)))

        popular = (rr.random() < 0.22)
        cap = capacity(level, rr, popular)

        status = "cancelled" if rr.random() < 0.01 else "scheduled"

        rows.append({
            "competition_id": int(comp_id),
            "name": f"{host_muni} {srow['sport_type_name'].title()} {start.year} #{comp_id}",
            "sport_type_id": sport_id,
            "host_club_id": host_club_id,
            "address_id": address_id,
            "venue": rr.choice(venues),
            "level": level,
            "start_date": start,
            "end_date": end,
            "status": status,
            "created_at": created_at,
            "registration_deadline": reg_deadline,
            "capacity": int(cap),
            "updated_at": datetime.combine(RUN_DATE, datetime.min.time()) + timedelta(hours=rr.randint(7,20), minutes=rr.randint(0,59))
        })

    df_new = spark.createDataFrame(rows)
    df_new_competitions = df_new  # Store at notebook level for Cell 5
    merge_into("competitions", df_new, ["competition_id"])

    if not already:
        record_state(TABLE, start_id, start_id+n_new, n_new, notes=f"Inserted {n_new} competitions (p_has={p_has:.2f})")

    display(df_new.orderBy(F.col("start_date").desc()).limit(20))

# COMMAND ----------

# DBTITLE 1,Cell 5
# Export competitions via the shared helper (overwrite per run_date partition — idempotent).
export_to_landing(TABLE, df_new_competitions)