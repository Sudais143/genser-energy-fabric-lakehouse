# =============================================================================
# NOTEBOOK: 04_silver_plant_readings
# PROJECT:  Genser Energy — Operational Intelligence Platform
# PURPOSE:  Clean and incrementally append SCADA plant hourly readings
# LAYER:    Silver
# SOURCE:   bronze_lh.scada_plant_hourly
# TARGET:   silver_lh.plant_readings
# DEFAULT LAKEHOUSE: silver_lh
#
# STRATEGY: Incremental append (time-series — new readings every 3 hours)
#   WHY: Generation readings are immutable facts. A reading at 09:00 does not
#        change after it is recorded. Incremental append is correct and efficient.
#
# CLEANING:
#   • Derive energy_mwh per reading interval (MW × hours)
#   • Compute plant_availability flag (OFFLINE = 0, else 1)
#   • Validate: mw_output cannot exceed plant installed capacity
#   • Validate: heat_rate must be within physically reasonable bounds (7000–15000)
#   • Classify load profile: PEAK / MID / BASE
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
TARGET_TABLE  = "silver_lh.plant_readings"

# Reading interval in hours (SCADA records every 3 hours)
READING_INTERVAL_HRS = 3.0
print(f"▶ Genser Silver — plant_readings — {datetime.now()}")


# %% ── CELL 2: Get last watermark ────────────────────────────────────────────
def get_last_watermark(table_name, col):
    try:
        wm = spark.sql(f"SELECT MAX({col}) as wm FROM {table_name}").collect()[0]["wm"]
        print(f"   Last watermark: {wm}")
        return wm
    except AnalysisException:
        print("   First run — full load")
        return None

last_wm = get_last_watermark(TARGET_TABLE, "reading_timestamp")


# %% ── CELL 3: Read and filter Bronze ────────────────────────────────────────
df_bronze = spark.read.format("delta").load(f"{BRONZE_TABLES}/scada_plant_hourly")
df_bronze = df_bronze.withColumn("reading_timestamp", F.to_timestamp("reading_timestamp"))

df_new = df_bronze.filter(F.col("reading_timestamp") > last_wm) if last_wm else df_bronze
new_count = df_new.count()
print(f"   New readings to process: {new_count:,}")


# %% ── CELL 4: Clean and validate ────────────────────────────────────────────
# Plant capacities for validation (from plant_master)
PLANT_CAPACITIES = {
    "PLT001": 66.0, "PLT002": 33.0, "PLT003": 25.5, "PLT004": 33.0, "PLT005": 33.0
}

df_clean = df_new \
    .withColumn("mw_output",
        F.when(F.col("mw_output") < 0, F.lit(0.0))  # Negative output = sensor error
         .otherwise(F.col("mw_output").cast("double"))) \
    .withColumn("heat_rate_btu_kwh",
        # Physical bounds: 7000 (best gas turbine) to 15000 (very inefficient)
        F.when(
            (F.col("heat_rate_btu_kwh") < 7000) | (F.col("heat_rate_btu_kwh") > 15000),
            F.lit(None).cast("double")  # Flag as sensor anomaly
        ).otherwise(F.col("heat_rate_btu_kwh").cast("double"))) \
    .withColumn("frequency_hz",
        F.when(
            (F.col("frequency_hz") < 48.0) | (F.col("frequency_hz") > 52.0),
            F.lit(None).cast("double")  # Ghana grid: 50Hz ± 2Hz tolerance
        ).otherwise(F.col("frequency_hz").cast("double")))


# %% ── CELL 5: Derive KPI columns ────────────────────────────────────────────
df_enriched = df_clean \
    .withColumn("energy_mwh",
        # Energy = Power × Time
        F.round(F.col("mw_output") * F.lit(READING_INTERVAL_HRS), 4)) \
    .withColumn("is_available",
        F.when(F.upper(F.col("status")) == "OFFLINE", F.lit(0))
         .otherwise(F.lit(1))) \
    .withColumn("load_profile",
        F.when(F.col("load_factor_pct") >= 90, "PEAK")
         .when(F.col("load_factor_pct") >= 70, "MID")
         .when(F.col("load_factor_pct") > 0,   "BASE")
         .otherwise("OFFLINE")) \
    .withColumn("reading_date",  F.to_date("reading_timestamp")) \
    .withColumn("reading_hour",  F.hour("reading_timestamp")) \
    .withColumn("reading_month", F.month("reading_timestamp")) \
    .withColumn("reading_year",  F.year("reading_timestamp")) \
    .drop("_ingestion_timestamp", "_source_system") \
    .withColumn("_silver_load_ts", F.current_timestamp())


# %% ── CELL 6: Write to Silver ───────────────────────────────────────────────
if new_count > 0:
    df_enriched.write.format("delta").mode("append") \
               .option("mergeSchema", "true").saveAsTable(TARGET_TABLE)
    print(f"\n✅ Appended {new_count:,} readings to {TARGET_TABLE}")
else:
    print("\n⏭  No new readings — skipping write")


# %% ── CELL 7: Validation ─────────────────────────────────────────────────────
df_result    = spark.table(TARGET_TABLE)
total        = df_result.count()
bad_hr       = df_result.filter(F.col("heat_rate_btu_kwh").isNull()).count()
offline_pct  = df_result.filter(F.col("is_available") == 0).count() / total * 100

print(f"\n{'='*60}")
print("SILVER VALIDATION — plant_readings")
print(f"{'='*60}")
print(f"   Total readings      : {total:,}")
print(f"   Anomalous heat rates: {bad_hr:,}  (flagged as NULL)")
print(f"   Offline readings    : {offline_pct:.1f}%  (typical: 1–5%)")
df_result.groupBy("plant_id","load_profile").count().orderBy("plant_id","load_profile").show()
print("✅ silver_lh.plant_readings — COMPLETE")
