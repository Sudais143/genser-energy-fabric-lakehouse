# =============================================================================
# NOTEBOOK: 06_gold_dim_plants
# PROJECT:  Genser Energy — Operational Intelligence Platform
# PURPOSE:  Build the Gold plant dimension
# LAYER:    Gold
# SOURCES:  silver_lh.plant_master (SCD2) | silver_lh.contracts (SCD2)
# TARGET:   gold_lh.dim_plants
# DEFAULT LAKEHOUSE: gold_lh
#
# LOGIC:
#   Join current plant records with their active PPA contract to produce
#   a single denormalised dimension row per plant.
#   dim_plants is the "who we are and who we serve" table.
#   Every generation and billing fact joins back to this.
# =============================================================================

# %% ── CELL 1: Imports ───────────────────────────────────────────────────────
from pyspark.sql import functions as F
from datetime import datetime

WORKSPACE_NAME   = "genser_energy_ws"   # ← UPDATE THIS
SILVER_LAKEHOUSE = "silver_lh"

def lakehouse_tables_path(name):
    return f"abfss://{WORKSPACE_NAME}@onelake.dfs.fabric.microsoft.com/{name}.Lakehouse/Tables"

SILVER_TABLES = lakehouse_tables_path(SILVER_LAKEHOUSE)
TARGET_TABLE  = "gold_lh.dim_plants"
print(f"▶ Genser Gold — dim_plants — {datetime.now()}")


# %% ── CELL 2: Read Silver ────────────────────────────────────────────────────
df_plants    = spark.read.format("delta").load(f"{SILVER_TABLES}/plant_master") \
                    .filter(F.col("is_current") == True)
df_contracts = spark.read.format("delta").load(f"{SILVER_TABLES}/contracts") \
                    .filter(F.col("is_current") == True)

print(f"   Current plants    : {df_plants.count()}")
print(f"   Current contracts : {df_contracts.count()}")


# %% ── CELL 3: Join and build dim_plants ─────────────────────────────────────
df_dim = df_plants \
    .join(
        df_contracts.select(
            "plant_id", "contract_id", "contract_type",
            "contracted_capacity_mw", "tariff_usd_per_mwh",
            "contract_start_date", "contract_end_date",
            "annual_contracted_revenue_usd"
        ),
        on="plant_id",
        how="left"
    ) \
    .withColumn("utilisation_capacity_pct",
        # What % of installed capacity is contracted?
        F.round(F.col("contracted_capacity_mw") / F.col("installed_capacity_mw") * 100, 1)) \
    .select(
        F.col("skey").alias("plant_key"),
        F.col("plant_id"),
        F.col("plant_name"),
        F.col("location"),
        F.col("region"),
        F.col("installed_capacity_mw"),
        F.col("available_capacity_mw"),
        F.col("fuel_type"),
        F.col("client_id"),
        F.col("client_name"),
        F.col("commissioning_date"),
        F.col("last_upgrade_date"),
        F.col("contract_id"),
        F.col("contract_type"),
        F.col("contracted_capacity_mw"),
        F.col("tariff_usd_per_mwh"),
        F.col("contract_start_date"),
        F.col("contract_end_date"),
        F.col("annual_contracted_revenue_usd"),
        F.col("utilisation_capacity_pct"),
        F.current_timestamp().alias("_gold_load_ts")
    )

print(f"\n   dim_plants rows: {df_dim.count()}")
df_dim.select("plant_id","plant_name","installed_capacity_mw","client_name","tariff_usd_per_mwh").show(truncate=False)


# %% ── CELL 4: Write to Gold ─────────────────────────────────────────────────
df_dim.write.format("delta").mode("overwrite") \
      .option("overwriteSchema", "true").saveAsTable(TARGET_TABLE)
print(f"✅ Written to {TARGET_TABLE}")


# %% ── CELL 5: Validation ─────────────────────────────────────────────────────
df_result = spark.table(TARGET_TABLE)
total     = df_result.count()
no_contract = df_result.filter(F.col("contract_id").isNull()).count()
print(f"\n{'='*60}")
print("GOLD VALIDATION — dim_plants")
print(f"{'='*60}")
print(f"   Total plant records     : {total}")
print(f"   Plants without contract : {no_contract}  (should be 0)")
assert no_contract == 0, "❌ Plants found with no active contract!"
print("✅ gold_lh.dim_plants — COMPLETE")
