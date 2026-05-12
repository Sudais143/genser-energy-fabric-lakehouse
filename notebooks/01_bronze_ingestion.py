# =============================================================================
# NOTEBOOK: 01_bronze_ingestion
# PROJECT:  Genser Energy — Operational Intelligence Platform
# PURPOSE:  Ingest raw source files into Bronze Delta tables
# LAYER:    Bronze
# DEFAULT LAKEHOUSE: bronze_lh
#
# SOURCES:
#   SCADA system  → scada_plant_hourly, scada_equipment_alarms
#   SAP/ERP       → erp_plant_master, erp_contracts, erp_invoices, erp_fuel_deliveries
#
# STRATEGY:
#   • SCADA tables      → incremental (time-series, grows every 3 hours)
#   • ERP invoices      → incremental (new invoices monthly)
#   • ERP fuel          → incremental (new deliveries every 2 days)
#   • ERP plant/contracts → full overwrite (small reference tables, <10 rows)
#   • Every row tagged with _ingestion_timestamp and _source_system
#   • Bronze = permanent archive. Nothing is ever changed or deleted here.
# =============================================================================

# %% ── CELL 1: Imports & Config ──────────────────────────────────────────────
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException
from datetime import datetime

WORKSPACE_NAME   = "genser_energy_ws"   # ← UPDATE THIS
BRONZE_LAKEHOUSE = "bronze_lh"

def lakehouse_tables_path(name):
    return f"abfss://{WORKSPACE_NAME}@onelake.dfs.fabric.microsoft.com/{name}.Lakehouse/Tables"

BRONZE_TABLES = lakehouse_tables_path(BRONZE_LAKEHOUSE)
SOURCE_BASE   = "Files/source_data"

print(f"▶ Genser Energy Bronze Ingestion — {datetime.now()}")
print(f"  Workspace : {WORKSPACE_NAME}")
print(f"  Target    : {BRONZE_LAKEHOUSE}")


# %% ── CELL 2: Helper — last watermark ───────────────────────────────────────
def get_last_watermark(table_name: str, watermark_col: str):
    try:
        wm = spark.sql(f"SELECT MAX({watermark_col}) as wm FROM {table_name}").collect()[0]["wm"]
        print(f"   Last watermark [{table_name}]: {wm}")
        return wm
    except AnalysisException:
        print(f"   First run [{table_name}] — full load")
        return None


# %% ── CELL 3: Helper — tag with metadata ────────────────────────────────────
def tag_raw(df, source_system: str):
    return df \
        .withColumn("_ingestion_timestamp", F.current_timestamp()) \
        .withColumn("_source_system", F.lit(source_system))


# %% ── CELL 4: Helper — incremental load ─────────────────────────────────────
def incremental_load(table_name: str, source_file: str,
                     watermark_col: str, source_system: str):
    print(f"\n▶ Incremental: {table_name}")
    df = spark.read.option("header", True).option("inferSchema", True).csv(source_file)
    df = tag_raw(df, source_system)

    last_wm = get_last_watermark(table_name, watermark_col)
    df_new  = df.filter(F.col(watermark_col) > last_wm) if last_wm else df

    count = df_new.count()
    if count > 0:
        df_new.write.format("delta").mode("append") \
              .option("mergeSchema", "true").saveAsTable(table_name)
        print(f"   ✅ Appended {count:,} new rows")
    else:
        print(f"   ⏭  No new rows — skipping")


# %% ── CELL 5: Helper — overwrite load ───────────────────────────────────────
def overwrite_load(table_name: str, source_file: str, source_system: str):
    print(f"\n▶ Overwrite: {table_name}")
    df = spark.read.option("header", True).option("inferSchema", True).csv(source_file)
    df = tag_raw(df, source_system)
    count = df.count()
    df.write.format("delta").mode("overwrite").saveAsTable(table_name)
    print(f"   ✅ Loaded {count:,} rows")


# %% ── CELL 6: Run SCADA ingestion ───────────────────────────────────────────
print("\n" + "="*60)
print("SCADA INGESTION")
print("="*60)

incremental_load(
    table_name    = "scada_plant_hourly",
    source_file   = f"{SOURCE_BASE}/scada_plant_hourly.csv",
    watermark_col = "reading_timestamp",
    source_system = "SCADA"
)

incremental_load(
    table_name    = "scada_equipment_alarms",
    source_file   = f"{SOURCE_BASE}/scada_equipment_alarms.csv",
    watermark_col = "alarm_timestamp",
    source_system = "SCADA"
)


# %% ── CELL 7: Run ERP ingestion ─────────────────────────────────────────────
print("\n" + "="*60)
print("ERP INGESTION")
print("="*60)

overwrite_load("erp_plant_master",  f"{SOURCE_BASE}/erp_plant_master.csv",   "SAP_ERP")
overwrite_load("erp_contracts",     f"{SOURCE_BASE}/erp_contracts.csv",       "SAP_ERP")

incremental_load(
    table_name    = "erp_invoices",
    source_file   = f"{SOURCE_BASE}/erp_invoices.csv",
    watermark_col = "invoice_date",
    source_system = "SAP_ERP"
)

incremental_load(
    table_name    = "erp_fuel_deliveries",
    source_file   = f"{SOURCE_BASE}/erp_fuel_deliveries.csv",
    watermark_col = "delivery_date",
    source_system = "SAP_ERP"
)


# %% ── CELL 8: Validation ─────────────────────────────────────────────────────
print("\n" + "="*60)
print("BRONZE VALIDATION")
print("="*60)

tables = [
    "scada_plant_hourly", "scada_equipment_alarms",
    "erp_plant_master", "erp_contracts", "erp_invoices", "erp_fuel_deliveries"
]
for tbl in tables:
    try:
        count = spark.table(tbl).count()
        print(f"  {tbl:<35} {count:>8,} rows  ✅")
    except Exception as e:
        print(f"  ❌ {tbl}: {e}")

print(f"\n✅ BRONZE INGESTION COMPLETE — {datetime.now()}")
