# =============================================================================
# NOTEBOOK: 02_silver_plant_master
# PROJECT:  Genser Energy — Operational Intelligence Platform
# PURPOSE:  Clean and version plant master data using SCD Type 2
# LAYER:    Silver
# SOURCE:   bronze_lh.erp_plant_master
# TARGET:   silver_lh.plant_master
# DEFAULT LAKEHOUSE: silver_lh
#
# STRATEGY: SCD Type 2
#   WHY: Plant specs change — available capacity is de-rated after turbine inspections,
#        fuel type may transition (gas → hybrid), upgrade dates shift.
#        Without SCD2, we lose the ability to calculate whether a plant met its
#        *contracted* capacity at the time of a billing dispute.
#
#   Hash all business columns → if hash changes, close old record, open new one
#   Deterministic surrogate key = SHA-256(plant_id + effective_start_date)
#
# DATA QUALITY:
#   • Null plant_id → quarantine
#   • available_capacity > installed_capacity → flag (physically impossible)
#   • Missing region → default to "Ghana" (all plants are in Ghana)
# =============================================================================

# %% ── CELL 1: Imports ───────────────────────────────────────────────────────
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from pyspark.sql.utils import AnalysisException
from datetime import datetime

WORKSPACE_NAME   = "genser_energy_ws"   # ← UPDATE THIS
BRONZE_LAKEHOUSE = "bronze_lh"

def lakehouse_tables_path(name):
    return f"abfss://{WORKSPACE_NAME}@onelake.dfs.fabric.microsoft.com/{name}.Lakehouse/Tables"

BRONZE_TABLES = lakehouse_tables_path(BRONZE_LAKEHOUSE)
TARGET_TABLE  = "silver_lh.plant_master"
today         = datetime.today().strftime("%Y-%m-%d")
print(f"▶ Genser Silver — plant_master — {datetime.now()}")


# %% ── CELL 2: Read Bronze ───────────────────────────────────────────────────
df_bronze = spark.read.format("delta").load(f"{BRONZE_TABLES}/erp_plant_master")
print(f"   Bronze rows: {df_bronze.count()}")


# %% ── CELL 3: Data Quality — quarantine nulls ───────────────────────────────
df_nulls = df_bronze.filter(F.col("plant_id").isNull())
df_clean = df_bronze.filter(F.col("plant_id").isNotNull())
null_count = df_nulls.count()
if null_count > 0:
    df_nulls.withColumn("dq_reason",    F.lit("null_business_key")) \
            .withColumn("dq_timestamp", F.current_timestamp()) \
            .withColumn("dq_source",    F.lit("silver_plant_master")) \
            .write.format("delta").mode("append") \
            .option("mergeSchema", "true").saveAsTable("dq_quarantine")
    print(f"   ⚠ {null_count} null plant_id rows quarantined")
else:
    print("   ✅ No null business keys")


# %% ── CELL 4: Clean and validate ────────────────────────────────────────────
df_clean2 = df_clean \
    .withColumn("plant_name",
        F.initcap(F.trim(F.col("plant_name")))) \
    .withColumn("region",
        F.coalesce(
            F.when(F.trim(F.col("region")) == "", F.lit(None)).otherwise(F.col("region")),
            F.lit("Ghana")          # All Genser plants are in Ghana
        )) \
    .withColumn("available_capacity_mw",
        # Physically impossible: available > installed → cap at installed
        F.when(
            F.col("available_capacity_mw") > F.col("installed_capacity_mw"),
            F.col("installed_capacity_mw")
        ).otherwise(F.col("available_capacity_mw"))) \
    .withColumn("commissioning_date", F.to_date(F.col("commissioning_date"))) \
    .withColumn("last_upgrade_date",  F.to_date(F.col("last_upgrade_date"))) \
    .drop("_ingestion_timestamp", "_source_system")

print("   ✅ Cleaning complete")


# %% ── CELL 5: Hash for change detection ─────────────────────────────────────
BUSINESS_COLS = [
    "plant_name", "location", "region", "installed_capacity_mw",
    "available_capacity_mw", "fuel_type", "client_id", "client_name", "last_upgrade_date"
]
df_hashed = df_clean2.withColumn(
    "_row_hash",
    F.md5(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in BUSINESS_COLS]))
)


# %% ── CELL 6: SCD Type 2 MERGE ──────────────────────────────────────────────
print("\n▶ Applying SCD Type 2 MERGE ...")
try:
    spark.table(TARGET_TABLE)
    table_exists = True
except AnalysisException:
    table_exists = False

if not table_exists:
    df_initial = df_hashed \
        .withColumn("skey",
            F.sha2(F.concat_ws("||", F.col("plant_id"), F.lit(today)), 256)) \
        .withColumn("effective_start_date", F.lit(today).cast("date")) \
        .withColumn("effective_end_date",   F.lit("9999-12-31").cast("date")) \
        .withColumn("is_current",           F.lit(True)) \
        .withColumn("_silver_load_ts",      F.current_timestamp())
    df_initial.write.format("delta").mode("overwrite").saveAsTable(TARGET_TABLE)
    print(f"   ✅ Initial load: {df_initial.count()} plant records")
else:
    delta_target = DeltaTable.forName(spark, TARGET_TABLE)
    df_existing  = spark.table(TARGET_TABLE).filter(F.col("is_current") == True).select("plant_id", "_row_hash")
    df_changed   = df_hashed.join(df_existing, on="plant_id", how="left").filter(
        df_existing["_row_hash"].isNull() | (df_hashed["_row_hash"] != df_existing["_row_hash"])
    ).drop(df_existing["_row_hash"])

    changed_count = df_changed.count()
    print(f"   Changed/new plants: {changed_count}")
    if changed_count > 0:
        changed_ids = [r["plant_id"] for r in df_changed.select("plant_id").collect()]
        delta_target.update(
            condition = (F.col("is_current") == True) & F.col("plant_id").isin(changed_ids),
            set = {"is_current": "false", "effective_end_date": f"'{today}'"}
        )
        df_new = df_changed \
            .withColumn("skey", F.sha2(F.concat_ws("||", F.col("plant_id"), F.lit(today)), 256)) \
            .withColumn("effective_start_date", F.lit(today).cast("date")) \
            .withColumn("effective_end_date",   F.lit("9999-12-31").cast("date")) \
            .withColumn("is_current",           F.lit(True)) \
            .withColumn("_silver_load_ts",      F.current_timestamp())
        df_new.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(TARGET_TABLE)
        print(f"   ✅ {changed_count} plant versions updated (SCD2)")
    else:
        print("   ⏭  No changes detected")


# %% ── CELL 7: Validation ─────────────────────────────────────────────────────
df_result  = spark.table(TARGET_TABLE)
total      = df_result.count()
current    = df_result.filter(F.col("is_current") == True).count()
cap_issues = df_result.filter(F.col("available_capacity_mw") > F.col("installed_capacity_mw")).count()

print(f"\n{'='*60}")
print("SILVER VALIDATION — plant_master")
print(f"{'='*60}")
print(f"   Total records     : {total}")
print(f"   Current records   : {current} (expect 5 — one per plant)")
print(f"   Capacity anomalies: {cap_issues}  (should be 0)")
df_result.filter(F.col("is_current") == True).select(
    "plant_id","plant_name","available_capacity_mw","client_name","is_current"
).show(truncate=False)
assert cap_issues == 0, "❌ Available capacity exceeds installed capacity!"
print("✅ silver_lh.plant_master — COMPLETE")
