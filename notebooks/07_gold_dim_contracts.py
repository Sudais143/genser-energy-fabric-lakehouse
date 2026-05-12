# =============================================================================
# NOTEBOOK: 07_gold_dim_contracts
# PROJECT:  Genser Energy — Operational Intelligence Platform
# PURPOSE:  Build the Gold contract dimension (all versions, not just current)
# LAYER:    Gold
# SOURCE:   silver_lh.contracts
# TARGET:   gold_lh.dim_contracts
# DEFAULT LAKEHOUSE: gold_lh
#
# WHY THIS TABLE EXISTS SEPARATELY FROM dim_plants:
#   dim_plants carries the CURRENT contract snapshot — for real-time reporting.
#   dim_contracts carries ALL contract versions — for historical billing analysis
#   and regulatory compliance. When an auditor asks "what was the tariff for
#   Tarkwa in 2022?", they query dim_contracts, not dim_plants.
#
# This is the primary table that proves the SCD2 design delivers business value.
# =============================================================================

# %% ── CELL 1: Imports ───────────────────────────────────────────────────────
from pyspark.sql import functions as F
from datetime import datetime

WORKSPACE_NAME   = "genser_energy_ws"   # ← UPDATE THIS
SILVER_LAKEHOUSE = "silver_lh"

def lakehouse_tables_path(name):
    return f"abfss://{WORKSPACE_NAME}@onelake.dfs.fabric.microsoft.com/{name}.Lakehouse/Tables"

SILVER_TABLES = lakehouse_tables_path(SILVER_LAKEHOUSE)
TARGET_TABLE  = "gold_lh.dim_contracts"
print(f"▶ Genser Gold — dim_contracts — {datetime.now()}")


# %% ── CELL 2: Read ALL contract versions from Silver ────────────────────────
# Note: we take ALL records — not just is_current == True
# This gives the full history for auditing and billing reconciliation
df_contracts = spark.read.format("delta").load(f"{SILVER_TABLES}/contracts")
df_plants    = spark.read.format("delta").load(f"{SILVER_TABLES}/plant_master") \
                    .filter(F.col("is_current") == True) \
                    .select("plant_id", "plant_name", "location", "region")

print(f"   Contract versions  : {df_contracts.count()}")
print(f"   Current plants     : {df_plants.count()}")


# %% ── CELL 3: Enrich and select ─────────────────────────────────────────────
df_dim = df_contracts \
    .join(df_plants, on="plant_id", how="left") \
    .withColumn("contract_status",
        F.when(F.col("is_current") == True,  "Active")
         .when(
             F.col("contract_end_date") < F.current_date(),
             "Expired"
         ).otherwise("Superseded")) \
    .withColumn("months_remaining",
        F.when(
            F.col("is_current") == True,
            F.round(F.datediff(F.col("contract_end_date"), F.current_date()) / 30.44, 1)
        ).otherwise(F.lit(None).cast("double"))) \
    .select(
        F.col("skey").alias("contract_key"),
        F.col("contract_id"),
        F.col("client_id"),
        F.col("client_name"),
        F.col("plant_id"),
        F.col("plant_name"),
        F.col("location").alias("plant_location"),
        F.col("region").alias("plant_region"),
        F.col("contract_type"),
        F.col("contracted_capacity_mw"),
        F.col("tariff_usd_per_mwh"),
        F.col("contract_start_date"),
        F.col("contract_end_date"),
        F.col("contract_duration_years"),
        F.col("annual_contracted_revenue_usd"),
        F.col("billing_cycle"),
        F.col("currency"),
        F.col("contract_status"),
        F.col("months_remaining"),
        F.col("effective_start_date"),
        F.col("effective_end_date"),
        F.col("is_current"),
        F.current_timestamp().alias("_gold_load_ts")
    )


# %% ── CELL 4: Write to Gold ─────────────────────────────────────────────────
df_dim.write.format("delta").mode("overwrite") \
      .option("overwriteSchema", "true").saveAsTable(TARGET_TABLE)
print(f"✅ Written to {TARGET_TABLE}")


# %% ── CELL 5: Validation ─────────────────────────────────────────────────────
df_result = spark.table(TARGET_TABLE)
total     = df_result.count()
active    = df_result.filter(F.col("contract_status") == "Active").count()
historical= df_result.filter(F.col("contract_status") != "Active").count()

print(f"\n{'='*60}")
print("GOLD VALIDATION — dim_contracts")
print(f"{'='*60}")
print(f"   Total contract versions : {total}")
print(f"   Active PPAs             : {active}")
print(f"   Historical (SCD2)       : {historical}  ← this is the SCD2 value")
df_result.select("contract_id","client_name","tariff_usd_per_mwh","contract_status","is_current") \
         .orderBy("contract_id","is_current").show(truncate=False)
print("✅ gold_lh.dim_contracts — COMPLETE")
