# Databricks notebook source
# 16_membership_payments_daily — create payment attempts for new memberships + settle invoices + retry failures
# Writes: membership_payments

# COMMAND ----------

from datetime import date

dbutils.widgets.removeAll()
dbutils.widgets.text("run_date",date.today().isoformat() )          # <- Monday
dbutils.widgets.text("volume", "medium")                # low|medium|high
dbutils.widgets.text("volume_factor", "1.0")            # scales volume_mult

# COMMAND ----------

# MAGIC %run ./00_utils

# COMMAND ----------

import random
from datetime import timedelta, datetime
from pyspark.sql import functions as F

TABLE = "membership_payments"
rnd = random.Random(seed_for(TABLE))

payments_tbl = spark.table(tbl("membership_payments"))
memberships_tbl = spark.table(tbl("memberships")).select("membership_id","member_id","price_nok","start_date")

# COMMAND ----------

# DBTITLE 1,Cell 5
# 1) Create payment attempt #1 for memberships that start today and have price > 0 and no payments yet
df_new_payments = None  # Initialize at notebook level

today_memberships = (memberships_tbl.where(F.col("start_date") == F.lit(RUN_DATE))
                                 .where(F.col("price_nok") > F.lit(0)))

has_payment = payments_tbl.select("membership_id").distinct()

to_pay = today_memberships.join(has_payment, on="membership_id", how="left_anti")

rows=[]
for m in to_pay.collect():
    midship = int(m["membership_id"])
    member_id = int(m["member_id"])
    amount = int(m["price_nok"])
    rr = random.Random(seed_for(TABLE) + midship)

    method = rr.choices(["vipps","card","invoice"], weights=[0.45,0.35,0.20])[0]
    # invoice: more pending; others mostly paid
    if method == "invoice":
        status = "pending" if rr.random() < 0.65 else "paid"
    else:
        status = rr.choices(["paid","failed","refunded"], weights=[0.965,0.025,0.010])[0]

    created_ts = datetime.combine(RUN_DATE, datetime.min.time()) + timedelta(hours=rr.randint(7,19), minutes=rr.randint(0,59))
    paid_at = None
    if status == "paid":
        # paid within 0-5 days
        paid_at = created_ts + timedelta(days=rr.randint(0,5), hours=rr.randint(0,23), minutes=rr.randint(0,59))

    rows.append({
        "payment_id": midship*10 + 1,
        "membership_id": midship,
        "member_id": member_id,
        "amount_nok": amount,
        "currency": "NOK",
        "method": method,
        "status": status,
        "attempt": 1,
        "created_at": created_ts,
        "paid_at": paid_at,
        "updated_at": created_ts
    })

if rows:
    df_new = spark.createDataFrame(rows)
    df_new_payments = df_new  # Store at notebook level for Cell 8
    merge_into("membership_payments", df_new, ["payment_id"])
    display(df_new.limit(20))
else:
    print("No new payments to create today.")

# COMMAND ----------

# 2) Settle a share of pending invoices (created 2–14 days ago)
pending = (payments_tbl.where(F.col("status") == F.lit("pending"))
                      .where(F.col("method") == F.lit("invoice"))
                      .where(F.col("created_at") < F.lit(datetime.combine(RUN_DATE - timedelta(days=2), datetime.min.time())))
                      .limit(int(2000 * VOLUME_MULT)))  # cap workload

pending_rows = pending.collect()
settle = []
for p in pending_rows:
    rr = random.Random(seed_for(TABLE) + int(p["payment_id"]))
    if rr.random() < 0.35:  # 35% of eligible pending settle per day
        settle.append(int(p["payment_id"]))

if settle:
    settle_list = ",".join(str(x) for x in settle)
    spark.sql(f"""
      UPDATE {tbl("membership_payments")}
      SET status = 'paid',
          paid_at = current_timestamp(),
          updated_at = current_timestamp()
      WHERE payment_id IN ({settle_list})
    """)
    print(f"Settled {len(settle)} pending invoices.")
else:
    print("No pending invoices settled today.")

# COMMAND ----------

# DBTITLE 1,Cell 7
# 3) Retry a small share of failed payments (attempt < 3, created last 14 days)
df_retry_payments = None  # Initialize at notebook level

failed = (payments_tbl.where(F.col("status") == F.lit("failed"))
                    .where(F.col("attempt") < F.lit(3))
                    .where(F.col("created_at") >= F.lit(datetime.combine(RUN_DATE - timedelta(days=14), datetime.min.time())))
                    .limit(int(2000 * VOLUME_MULT)))

failed_rows = failed.collect()
retry_rows=[]
for p in failed_rows:
    rr = random.Random(seed_for(TABLE) + int(p["payment_id"]) + 555)
    if rr.random() > 0.25:  # only retry some
        continue
    membership_id = int(p["membership_id"])
    attempt = int(p["attempt"]) + 1
    payment_id = membership_id*10 + attempt
    # make retries more likely to succeed
    status = rr.choices(["paid","failed"], weights=[0.92,0.08])[0]
    created_ts = datetime.combine(RUN_DATE, datetime.min.time()) + timedelta(hours=rr.randint(7,19), minutes=rr.randint(0,59))
    paid_at = created_ts + timedelta(hours=rr.randint(0,12), minutes=rr.randint(0,59)) if status == "paid" else None

    retry_rows.append({
        "payment_id": int(payment_id),
        "membership_id": membership_id,
        "member_id": int(p["member_id"]),
        "amount_nok": int(p["amount_nok"]),
        "currency": "NOK",
        "method": str(p["method"]),
        "status": status,
        "attempt": attempt,
        "created_at": created_ts,
        "paid_at": paid_at,
        "updated_at": created_ts
    })

if retry_rows:
    df_retry = spark.createDataFrame(retry_rows)
    df_retry_payments = df_retry  # Store at notebook level for Cell 8
    merge_into("membership_payments", df_retry, ["payment_id"])
    print(f"Retries created: {df_retry.count()}")
    display(df_retry.limit(20))
else:
    print("No retries created today.")

# COMMAND ----------

# DBTITLE 1,Cell 8
# Combine all payment transactions (new payments + retries), then export via the
# shared helper (overwrite per run_date partition — idempotent).
# Note: settled invoices (Cell 6) are UPDATE operations, not exported as new data.
df_combined = None
if df_new_payments is not None and df_retry_payments is not None:
    df_combined = df_new_payments.unionByName(df_retry_payments)
elif df_new_payments is not None:
    df_combined = df_new_payments
elif df_retry_payments is not None:
    df_combined = df_retry_payments

export_to_landing(TABLE, df_combined)