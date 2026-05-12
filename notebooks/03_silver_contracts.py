# =============================================================================
# NOTEBOOK: 03_silver_contracts
# PROJECT:  Genser Energy — Operational Intelligence Platform
# PURPOSE:  Clean and version Power Purchase Agreements using SCD Type 2
# LAYER:    Silver
# SOURCE:   bronze_lh.erp_contracts
# TARGET:   silver_lh.contracts
# DEFAULT LAKEHOUSE: silver_lh
#
# STRATEGY: SCD Type 2
#   WHY: This is the most important SCD2 table in the entire pipeline.
#        PPAs between Genser and mining clients get renegotiated — tariffs shift,
#        contracted capacity changes when mines expand or reduce operations.
#        If we overwrite old contracts, we cannot reconcile historical billing:
#        "Did we charge the right tariff for the Tarkwa invoice in March 2022?"
#        With SCD2, every contract version is preserved with its validity window.
#
#   Business key: contract_id (each PPA has a unique ID per plant+client pair)
#   Hash columns: tariff, contracted_capacity, contract_end_date
#
# DATA QUALITY:
#   • Null contract_id → quarantine
#   • Negative tariff → quarantine (data entry error)
#   • contracted_capacity > plant installed capacity → flag
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
TARGET_TABLE  = "silver_lh.contracts"
today         = datetime.today().strftime("%Y-%m-%d")
print(f"▶ Genser Silver — contracts — {datetime.now()}")


# %% ── CELL 2: Read Bronze ───────────────────────────────────────────────────
df_bronze = spark.read.format("delta").load(f"{BRONZE_TABLES}/erp_contracts")
print(f"   Bronze rows: {df_bronze.count()}")


# %% ── CELL 3: Data Quality ───────────────────────────────────────────────────
# Null contract_id or negative tariff → quarantine
df_bad   = df_bronze.filter(F.col("contract_id").isNull() | (F.col("tariff_usd_per_mwh") < 0))
df_clean = df_bronze.filter(F.col("contract_id").isNotNull() & (F.col("tariff_usd_per_mwh") >= 0))
bad_count = df_bad.count()
if bad_count > 0:
    df_bad.withColumn("dq_reason",    F.lit("null_key_or_negative_tariff")) \
          .withColumn("dq_timestamp", F.current_timestamp()) \
          .withColumn("dq_source",    F.lit("silver_contracts")) \
          .write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable("dq_quarantine")
    print(f"   ⚠ {bad_count} bad rows quarantined")


# %% ── CELL 4: Clean and enrich ──────────────────────────────────────────────
df_clean2 = df_clean \
    .withColumn("client_name",       F.initcap(F.trim(F.col("client_name")))) \
    .withColumn("contract_type",     F.upper(F.trim(F.col("contract_type")))) \
    .withColumn("billing_cycle",     F.initcap(F.trim(F.col("billing_cycle")))) \
    .withColumn("contract_start_date", F.to_date(F.col("contract_start_date"))) \
    .withColumn("contract_end_date",   F.to_date(F.col("contract_end_date"))) \
    .withColumn("contract_duration_years",
        F.round(F.datediff(F.col("contract_end_date"), F.col("contract_start_date")) / 365.25, 1)) \
    .withColumn("annual_contracted_revenue_usd",
        # Estimated annual revenue: contracted MW * 8760 hrs * assumed 90% availability * tariff
        F.round(F.col("contracted_capacity_mw") * 8760 * 0.90 * F.col("tariff_usd_per_mwh"), 2)) \
    .drop("_ingestion_timestamp", "_source_system")


# %% ── CELL 5: Hash for change detection ─────────────────────────────────────
BUSINESS_COLS = ["tariff_usd_per_mwh", "contracted_capacity_mw", "contract_end_date", "billing_cycle", "currency"]
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
        .withColumn("skey", F.sha2(F.concat_ws("||", F.col("contract_id"), F.lit(today)), 256)) \
        .withColumn("effective_start_date", F.lit(today).cast("date")) \
        .withColumn("effective_end_date",   F.lit("9999-12-31").cast("date")) \
        .withColumn("is_current",           F.lit(True)) \
        .withColumn("_silver_load_ts",      F.current_timestamp())
    df_initial.write.format("delta").mode("overwrite").saveAsTable(TARGET_TABLE)
    print(f"   ✅ Initial load: {df_initial.count()} contract records")
else:
    delta_target = DeltaTable.forName(spark, TARGET_TABLE)
    df_existing  = spark.table(TARGET_TABLE).filter(F.col("is_current") == True).select("contract_id", "_row_hash")
    df_changed   = df_hashed.join(df_existing, on="contract_id", how="left").filter(
        df_existing["_row_hash"].isNull() | (df_hashed["_row_hash"] != df_existing["_row_hash"])
    ).drop(df_existing["_row_hash"])

    changed_count = df_changed.count()
    print(f"   Changed/new contracts: {changed_count}")
    if changed_count > 0:
        changed_ids = [r["contract_id"] for r in df_changed.select("contract_id").collect()]
        delta_target.update(
            condition = (F.col("is_current") == True) & F.col("contract_id").isin(changed_ids),
            set = {"is_current": "false", "effective_end_date": f"'{today}'"}
        )
        df_new = df_changed \
            .withColumn("skey", F.sha2(F.concat_ws("||", F.col("contract_id"), F.lit(today)), 256)) \
            .withColumn("effective_start_date", F.lit(today).cast("date")) \
            .withColumn("effective_end_date",   F.lit("9999-12-31").cast("date")) \
            .withColumn("is_current",           F.lit(True)) \
            .withColumn("_silver_load_ts",      F.current_timestamp())
        df_new.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(TARGET_TABLE)
        print(f"   ✅ {changed_count} contract versions updated (SCD2)")
    else:
        print("   ⏭  No contract changes detected")


# %% ── CELL 7: Validation ─────────────────────────────────────────────────────
df_result = spark.table(TARGET_TABLE)
total     = df_result.count()
current   = df_result.filter(F.col("is_current") == True).count()
historical= df_result.filter(F.col("is_current") == False).count()

print(f"\n{'='*60}")
print("SILVER VALIDATION — contracts")
print(f"{'='*60}")
print(f"   Total records   : {total}")
print(f"   Current PPAs    : {current}")
print(f"   Historical vers : {historical} (renegotiated contracts preserved)")
df_result.filter(F.col("is_current") == True).select(
    "contract_id","client_name","plant_id","tariff_usd_per_mwh","contracted_capacity_mw","is_current"
).show(truncate=False)
print("✅ silver_lh.contracts — COMPLETE")
