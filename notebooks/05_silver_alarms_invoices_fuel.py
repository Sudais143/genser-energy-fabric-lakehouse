# =============================================================================
# NOTEBOOK: 05_silver_alarms_invoices_fuel
# PROJECT:  Genser Energy — Operational Intelligence Platform
# PURPOSE:  Clean equipment alarms, invoices, and fuel deliveries
# LAYER:    Silver
# SOURCES:  scada_equipment_alarms | erp_invoices | erp_fuel_deliveries
# TARGETS:  silver_lh.equipment_alarms | silver_lh.invoices | silver_lh.fuel_deliveries
# DEFAULT LAKEHOUSE: silver_lh
#
# STRATEGIES:
#   equipment_alarms  → incremental append (events accumulate over time)
#   invoices          → full overwrite    (12–60 rows, monthly billing, safe to reload)
#   fuel_deliveries   → incremental append (new deliveries every 2 days)
# =============================================================================

# %% ── CELL 1: Imports ───────────────────────────────────────────────────────
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException
from datetime import datetime

WORKSPACE_NAME   = "genser_energy_ws"   # ← UPDATE THIS
BRONZE_LAKEHOUSE = "bronze_lh"

def lakehouse_tables_path(name):
    return f"abfss://{WORKSPACE_NAME}@onelake.dfs.fabric.microsoft.com/{name}.Lakehouse/Tables"

BRONZE_TABLES = lakehouse_tables_path(BRONZE_LAKEHOUSE)
print(f"▶ Genser Silver — Alarms / Invoices / Fuel — {datetime.now()}")


# %% ══════════════════════════════════════════════════════════════════════════
# TABLE 1: equipment_alarms
# ══════════════════════════════════════════════════════════════════════════════
print("\n▶ Processing equipment_alarms ...")

def get_last_wm(table, col):
    try:
        return spark.sql(f"SELECT MAX({col}) as wm FROM {table}").collect()[0]["wm"]
    except AnalysisException:
        return None

last_wm_alarms = get_last_wm("silver_lh.equipment_alarms", "alarm_timestamp")

df_alarms = spark.read.format("delta").load(f"{BRONZE_TABLES}/scada_equipment_alarms") \
            .withColumn("alarm_timestamp",    F.to_timestamp("alarm_timestamp")) \
            .withColumn("resolved_timestamp", F.to_timestamp("resolved_timestamp"))

df_new_alarms = df_alarms.filter(F.col("alarm_timestamp") > last_wm_alarms) if last_wm_alarms else df_alarms

df_alarms_clean = df_new_alarms \
    .withColumn("severity",
        F.when(F.upper(F.col("severity")) == "CRITICAL", "CRITICAL")
         .when(F.upper(F.col("severity")) == "WARNING",  "WARNING")
         .when(F.upper(F.col("severity")) == "INFO",     "INFO")
         .otherwise("UNKNOWN")) \
    .withColumn("downtime_hrs",
        # Estimated downtime contribution per alarm
        F.when(F.col("severity") == "CRITICAL",
               F.round(F.col("duration_minutes") / 60.0, 3))
         .otherwise(F.lit(0.0))) \
    .withColumn("alarm_date",  F.to_date("alarm_timestamp")) \
    .withColumn("alarm_month", F.month("alarm_timestamp")) \
    .withColumn("alarm_year",  F.year("alarm_timestamp")) \
    .drop("_ingestion_timestamp", "_source_system") \
    .withColumn("_silver_load_ts", F.current_timestamp())

alarms_count = df_new_alarms.count()
if alarms_count > 0:
    df_alarms_clean.write.format("delta").mode("append") \
                   .option("mergeSchema", "true").saveAsTable("silver_lh.equipment_alarms")
    print(f"   ✅ equipment_alarms: {alarms_count:,} new rows appended")
    # DQ: Check for alarms with resolved_timestamp before alarm_timestamp
    dq_resolved = df_alarms_clean.filter(F.col("resolved_timestamp") < F.col("alarm_timestamp")).count()
    if dq_resolved > 0:
        print(f"   ⚠ {dq_resolved} alarms with resolved_time < alarm_time — check SCADA timestamps")
else:
    print("   ⏭  No new alarms")


# %% ══════════════════════════════════════════════════════════════════════════
# TABLE 2: invoices
# ══════════════════════════════════════════════════════════════════════════════
print("\n▶ Processing invoices ...")

df_invoices = spark.read.format("delta").load(f"{BRONZE_TABLES}/erp_invoices")

df_invoices_clean = df_invoices \
    .withColumn("invoice_date",  F.to_date("invoice_date")) \
    .withColumn("payment_date",  F.to_date("payment_date")) \
    .withColumn("payment_status",
        F.when(F.upper(F.col("payment_status")) == "PAID",        "Paid")
         .when(F.upper(F.col("payment_status")) == "OUTSTANDING", "Outstanding")
         .otherwise("Unknown")) \
    .withColumn("days_to_payment",
        F.when(F.col("payment_date").isNotNull(),
               F.datediff(F.col("payment_date"), F.col("invoice_date")))
         .otherwise(F.lit(None).cast("int"))) \
    .withColumn("is_overdue",
        # Outstanding AND invoice is older than 30 days
        F.when(
            (F.col("payment_status") == "Outstanding") &
            (F.datediff(F.current_date(), F.col("invoice_date")) > 30),
            F.lit(True)
        ).otherwise(F.lit(False))) \
    .drop("_ingestion_timestamp", "_source_system") \
    .withColumn("_silver_load_ts", F.current_timestamp())

df_invoices_clean.write.format("delta").mode("overwrite") \
                 .option("overwriteSchema", "true").saveAsTable("silver_lh.invoices")
print(f"   ✅ invoices: {df_invoices_clean.count()} rows written")


# %% ══════════════════════════════════════════════════════════════════════════
# TABLE 3: fuel_deliveries
# ══════════════════════════════════════════════════════════════════════════════
print("\n▶ Processing fuel_deliveries ...")

last_wm_fuel = get_last_wm("silver_lh.fuel_deliveries", "delivery_date")
df_fuel      = spark.read.format("delta").load(f"{BRONZE_TABLES}/erp_fuel_deliveries") \
               .withColumn("delivery_date", F.to_date("delivery_date"))
df_new_fuel  = df_fuel.filter(F.col("delivery_date") > last_wm_fuel) if last_wm_fuel else df_fuel

df_fuel_clean = df_new_fuel \
    .withColumn("total_cost_usd",
        F.when(F.col("total_cost_usd") < 0, F.lit(None).cast("double"))
         .otherwise(F.col("total_cost_usd").cast("double"))) \
    .withColumn("volume_mscf",
        F.when(F.col("volume_mscf") <= 0, F.lit(None).cast("double"))
         .otherwise(F.col("volume_mscf").cast("double"))) \
    .withColumn("delivery_month", F.month("delivery_date")) \
    .withColumn("delivery_year",  F.year("delivery_date")) \
    .drop("_ingestion_timestamp", "_source_system") \
    .withColumn("_silver_load_ts", F.current_timestamp())

fuel_count = df_new_fuel.count()
if fuel_count > 0:
    df_fuel_clean.write.format("delta").mode("append") \
                 .option("mergeSchema", "true").saveAsTable("silver_lh.fuel_deliveries")
    print(f"   ✅ fuel_deliveries: {fuel_count:,} new rows appended")
else:
    print("   ⏭  No new fuel deliveries")


# %% ── Final Validation ───────────────────────────────────────────────────────
print(f"\n{'='*60}")
print("SILVER VALIDATION — Alarms / Invoices / Fuel")
print(f"{'='*60}")
for tbl in ["silver_lh.equipment_alarms","silver_lh.invoices","silver_lh.fuel_deliveries"]:
    count = spark.table(tbl).count()
    print(f"   {tbl:<40} {count:>8,} rows  ✅")
print("✅ All 3 tables — COMPLETE")
