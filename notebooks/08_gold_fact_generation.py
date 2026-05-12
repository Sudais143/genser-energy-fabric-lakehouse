# =============================================================================
# NOTEBOOK: 08_gold_fact_generation
# PROJECT:  Genser Energy — Operational Intelligence Platform
# PURPOSE:  Build the Gold generation fact table — the core reporting table
# LAYER:    Gold
# SOURCES:  silver_lh.plant_readings    (hourly generation data)
#           silver_lh.invoices          (monthly billing)
#           silver_lh.fuel_deliveries   (fuel cost data)
#           gold_lh.dim_plants          (plant + contract dimension)
#           gold_lh.dim_contracts       (all contract versions)
# TARGETS:  gold_lh.fact_generation (hourly grain)
#           gold_lh.fact_monthly_performance (monthly rollup — for dashboards)
# DEFAULT LAKEHOUSE: gold_lh
#
# KEY METRICS ENABLED:
#   • Plant Availability Factor (%)  = hours available / total hours
#   • Capacity Factor (%)           = actual MWh / (installed MW × hours)
#   • Heat Rate (BTU/kWh)           = fuel efficiency indicator
#   • Revenue per MWh               = billing efficiency
#   • Fuel Cost per MWh             = operational cost efficiency
# =============================================================================

# %% ── CELL 1: Imports ───────────────────────────────────────────────────────
from pyspark.sql import functions as F
from datetime import datetime

WORKSPACE_NAME   = "genser_energy_ws"   # ← UPDATE THIS
SILVER_LAKEHOUSE = "silver_lh"
GOLD_LAKEHOUSE   = "gold_lh"

def lakehouse_tables_path(name):
    return f"abfss://{WORKSPACE_NAME}@onelake.dfs.fabric.microsoft.com/{name}.Lakehouse/Tables"

SILVER_TABLES = lakehouse_tables_path(SILVER_LAKEHOUSE)
GOLD_TABLES   = lakehouse_tables_path(GOLD_LAKEHOUSE)
print(f"▶ Genser Gold — fact_generation — {datetime.now()}")


# %% ── CELL 2: Read source tables ────────────────────────────────────────────
df_readings   = spark.read.format("delta").load(f"{SILVER_TABLES}/plant_readings")
df_plants     = spark.read.format("delta").load(f"{GOLD_TABLES}/dim_plants")
df_fuel       = spark.read.format("delta").load(f"{SILVER_TABLES}/fuel_deliveries")
df_invoices   = spark.read.format("delta").load(f"{SILVER_TABLES}/invoices")

print(f"   Hourly readings   : {df_readings.count():,}")
print(f"   Plants            : {df_plants.count()}")
print(f"   Fuel deliveries   : {df_fuel.count():,}")
print(f"   Invoices          : {df_invoices.count()}")


# %% ── CELL 3: Build hourly fact_generation ──────────────────────────────────
print("\n▶ Building fact_generation (hourly grain) ...")

df_fact = df_readings \
    .join(
        df_plants.select("plant_id", "plant_key", "installed_capacity_mw",
                          "available_capacity_mw", "contracted_capacity_mw",
                          "tariff_usd_per_mwh", "contract_id", "client_name"),
        on="plant_id",
        how="left"
    ) \
    .withColumn("theoretical_mwh",
        # Maximum possible energy in this interval
        F.round(F.col("installed_capacity_mw") * F.lit(3.0), 4)) \
    .withColumn("contracted_mwh",
        F.round(F.col("contracted_capacity_mw") * F.lit(3.0), 4)) \
    .withColumn("availability_flag", F.col("is_available").cast("double")) \
    .withColumn("billed_revenue_estimate_usd",
        # Estimated per-reading revenue contribution (actual billing is monthly)
        F.round(F.col("energy_mwh") * F.col("tariff_usd_per_mwh"), 2)) \
    .select(
        F.col("reading_id"),
        F.col("plant_id"),
        F.col("plant_key"),
        F.col("contract_id"),
        F.col("client_name"),
        F.col("reading_timestamp"),
        F.col("reading_date"),
        F.col("reading_year"),
        F.col("reading_month"),
        F.col("reading_hour"),
        F.col("mw_output"),
        F.col("energy_mwh"),
        F.col("theoretical_mwh"),
        F.col("contracted_mwh"),
        F.col("load_factor_pct"),
        F.col("load_profile"),
        F.col("heat_rate_btu_kwh"),
        F.col("fuel_consumed_mscf"),
        F.col("frequency_hz"),
        F.col("voltage_kv"),
        F.col("status"),
        F.col("is_available"),
        F.col("billed_revenue_estimate_usd"),
        F.current_timestamp().alias("_gold_load_ts")
    )

print(f"   fact_generation rows: {df_fact.count():,}")


# %% ── CELL 4: Build monthly rollup — fact_monthly_performance ───────────────
print("\n▶ Building fact_monthly_performance (monthly rollup) ...")

# Monthly fuel cost per plant (sum of deliveries in the month)
df_monthly_fuel = df_fuel \
    .withColumn("delivery_ym", F.date_format("delivery_date", "yyyy-MM")) \
    .groupBy("plant_id", "delivery_ym") \
    .agg(
        F.sum("volume_mscf").alias("total_fuel_mscf"),
        F.sum("total_cost_usd").alias("total_fuel_cost_usd")
    )

# Monthly performance rollup from hourly readings
df_monthly = df_fact \
    .withColumn("reading_ym", F.date_format("reading_date", "yyyy-MM")) \
    .groupBy("plant_id", "plant_key", "contract_id", "client_name", "reading_year", "reading_month", "reading_ym") \
    .agg(
        F.sum("energy_mwh").alias("total_energy_mwh"),
        F.sum("theoretical_mwh").alias("total_theoretical_mwh"),
        F.sum("contracted_mwh").alias("total_contracted_mwh"),
        F.sum("billed_revenue_estimate_usd").alias("estimated_revenue_usd"),
        F.avg("load_factor_pct").alias("avg_load_factor_pct"),
        F.avg("heat_rate_btu_kwh").alias("avg_heat_rate_btu_kwh"),
        F.sum("is_available").alias("available_readings"),
        F.count("reading_id").alias("total_readings")
    ) \
    .withColumn("availability_factor_pct",
        F.round(F.col("available_readings") / F.col("total_readings") * 100, 2)) \
    .withColumn("capacity_factor_pct",
        F.round(F.col("total_energy_mwh") / F.col("total_theoretical_mwh") * 100, 2)) \
    .withColumn("contracted_delivery_pct",
        # How much of what we're contractually obligated to deliver did we actually deliver?
        F.round(F.col("total_energy_mwh") / F.col("total_contracted_mwh") * 100, 2)) \
    .join(
        df_monthly_fuel,
        on=["plant_id"],
        how="left"
    ) \
    .withColumn("fuel_cost_per_mwh",
        F.when(F.col("total_energy_mwh") > 0,
               F.round(F.col("total_fuel_cost_usd") / F.col("total_energy_mwh"), 2))
         .otherwise(F.lit(None).cast("double"))) \
    .join(
        df_invoices.select("plant_id", "billing_period", "total_invoice_usd", "payment_status"),
        (df_monthly_fuel["plant_id"] == df_invoices["plant_id"]) &
        (F.col("reading_ym") == F.col("billing_period")),
        how="left"
    ) \
    .select(
        "plant_id", "plant_key", "contract_id", "client_name",
        "reading_year", "reading_month", "reading_ym",
        "total_energy_mwh", "total_theoretical_mwh", "total_contracted_mwh",
        "availability_factor_pct", "capacity_factor_pct", "contracted_delivery_pct",
        "avg_load_factor_pct", "avg_heat_rate_btu_kwh",
        "estimated_revenue_usd", "total_invoice_usd", "payment_status",
        "total_fuel_mscf", "total_fuel_cost_usd", "fuel_cost_per_mwh",
        F.current_timestamp().alias("_gold_load_ts")
    )

print(f"   fact_monthly_performance rows: {df_monthly.count()}")


# %% ── CELL 5: Write to Gold ─────────────────────────────────────────────────
df_fact.write.format("delta").mode("overwrite") \
       .option("overwriteSchema", "true").saveAsTable("gold_lh.fact_generation")
print(f"✅ Written: gold_lh.fact_generation")

df_monthly.write.format("delta").mode("overwrite") \
          .option("overwriteSchema", "true").saveAsTable("gold_lh.fact_monthly_performance")
print(f"✅ Written: gold_lh.fact_monthly_performance")


# %% ── CELL 6: Validation ─────────────────────────────────────────────────────
df_gen   = spark.table("gold_lh.fact_generation")
df_month = spark.table("gold_lh.fact_monthly_performance")

total_mwh      = df_gen.agg(F.sum("energy_mwh")).collect()[0][0]
null_plant_key = df_gen.filter(F.col("plant_key").isNull()).count()
date_range     = df_gen.agg(F.min("reading_date"), F.max("reading_date")).collect()[0]

print(f"\n{'='*60}")
print("GOLD VALIDATION — fact_generation")
print(f"{'='*60}")
print(f"   Total hourly readings   : {df_gen.count():,}")
print(f"   Total energy generated  : {total_mwh:,.1f} MWh")
print(f"   Null plant_key          : {null_plant_key}  (should be 0)")
print(f"   Date range              : {date_range[0]} → {date_range[1]}")

print(f"\n--- Monthly Performance Summary ---")
df_month.groupBy("plant_id").agg(
    F.avg("availability_factor_pct").alias("avg_availability_pct"),
    F.avg("capacity_factor_pct").alias("avg_capacity_factor_pct"),
    F.sum("total_energy_mwh").alias("total_mwh"),
    F.sum("total_invoice_usd").alias("total_billed_usd")
).orderBy("plant_id").show(truncate=False)

assert null_plant_key == 0, "❌ NULL plant keys in fact_generation!"
print(f"\n✅ Gold layer — COMPLETE")
print("="*60)
print("🏁 GENSER ENERGY PIPELINE — ALL LAYERS COMPLETE")
print("="*60)
