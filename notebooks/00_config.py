# =============================================================================
# NOTEBOOK: 00_config
# PROJECT:  Genser Energy — Operational Intelligence Platform
# PURPOSE:  Shared configuration for all pipeline notebooks
# =============================================================================
#
# BUSINESS CONTEXT:
#   Genser Energy operates 5 natural-gas-fired power plants across Ghana,
#   delivering power under long-term PPAs to major gold mining companies.
#   Data comes from two source systems:
#     • SCADA  — real-time plant telemetry (output, fuel, alarms)
#     • SAP/ERP — contracts, billing, fuel deliveries
#   This pipeline unifies both into a single medallion lakehouse so operations
#   and finance can answer questions like:
#     "What was Tarkwa's availability vs contracted capacity last quarter?"
#     "Which plant had the highest heat rate deviation in 2024?"
#     "How much revenue did we bill vs collect from Gold Fields YTD?"
# =============================================================================

# ── Fabric Workspace ──────────────────────────────────────────────────────────
WORKSPACE_NAME   = "genser_energy_ws"    # ← UPDATE to your exact workspace name

BRONZE_LAKEHOUSE = "bronze_lh"
SILVER_LAKEHOUSE = "silver_lh"
GOLD_LAKEHOUSE   = "gold_lh"

def lakehouse_path(name: str) -> str:
    return f"abfss://{WORKSPACE_NAME}@onelake.dfs.fabric.microsoft.com/{name}.Lakehouse"

BRONZE_PATH = lakehouse_path(BRONZE_LAKEHOUSE)
SILVER_PATH = lakehouse_path(SILVER_LAKEHOUSE)
GOLD_PATH   = lakehouse_path(GOLD_LAKEHOUSE)

# ── Source File Paths (CSV files uploaded to bronze_lh → Files/source_data/) ─
SOURCE_FILES = {
    "scada_plant_hourly":      "Files/source_data/scada_plant_hourly.csv",
    "scada_equipment_alarms":  "Files/source_data/scada_equipment_alarms.csv",
    "erp_plant_master":        "Files/source_data/erp_plant_master.csv",
    "erp_contracts":           "Files/source_data/erp_contracts.csv",
    "erp_invoices":            "Files/source_data/erp_invoices.csv",
    "erp_fuel_deliveries":     "Files/source_data/erp_fuel_deliveries.csv",
}

# ── Bronze table strategies ───────────────────────────────────────────────────
# SCADA tables: incremental (time-series — grows continuously)
# ERP reference tables: overwrite (small, safe to reload)
# ERP contracts: incremental (append new versions, preserve history for SCD2)
BRONZE_CONFIG = {
    "scada_plant_hourly":     {"strategy": "incremental", "watermark_col": "reading_timestamp"},
    "scada_equipment_alarms": {"strategy": "incremental", "watermark_col": "alarm_timestamp"},
    "erp_plant_master":       {"strategy": "overwrite"},
    "erp_contracts":          {"strategy": "overwrite"},   # Full reload — source is small
    "erp_invoices":           {"strategy": "incremental", "watermark_col": "invoice_date"},
    "erp_fuel_deliveries":    {"strategy": "incremental", "watermark_col": "delivery_date"},
}

# ── Silver table strategies ───────────────────────────────────────────────────
SILVER_CONFIG = {
    "plant_readings":    {"strategy": "incremental",  "source": "scada_plant_hourly",     "watermark_col": "reading_timestamp"},
    "equipment_alarms":  {"strategy": "incremental",  "source": "scada_equipment_alarms", "watermark_col": "alarm_timestamp"},
    "plant_master":      {"strategy": "scd2",         "source": "erp_plant_master",        "business_key": "plant_id"},
    "contracts":         {"strategy": "scd2",         "source": "erp_contracts",           "business_key": "contract_id"},
    "invoices":          {"strategy": "overwrite",    "source": "erp_invoices"},
    "fuel_deliveries":   {"strategy": "incremental",  "source": "erp_fuel_deliveries",     "watermark_col": "delivery_date"},
}

# ── Reference: Genser Plants ─────────────────────────────────────────────────
PLANTS = {
    "PLT001": {"name": "Tarkwa",  "capacity_mw": 58.0,  "client": "Gold Fields Limited"},
    "PLT002": {"name": "Chirano", "capacity_mw": 31.0,  "client": "Kinross Gold Corporation"},
    "PLT003": {"name": "Damang",  "capacity_mw": 25.5,  "client": "Gold Fields Limited"},
    "PLT004": {"name": "Edikan",  "capacity_mw": 33.0,  "client": "Perseus Mining Limited"},
    "PLT005": {"name": "Wassa",   "capacity_mw": 32.0,  "client": "Golden Star Resources"},
}

print("✅ Genser Energy config loaded")
print(f"   Bronze : {BRONZE_LAKEHOUSE}")
print(f"   Silver : {SILVER_LAKEHOUSE}")
print(f"   Gold   : {GOLD_LAKEHOUSE}")
print(f"   Plants : {len(PLANTS)} operational sites")
