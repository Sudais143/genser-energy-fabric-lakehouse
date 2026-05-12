import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random, math, os

random.seed(42)
np.random.seed(42)
OUT = "/sessions/great-awesome-darwin/genser_fabric/data"

# ── Plant master ─────────────────────────────────────────────────────────────
plants = pd.DataFrame([
    {"plant_id":"PLT001","plant_name":"Tarkwa Power Plant",  "location":"Tarkwa",  "region":"Western Region",
     "installed_capacity_mw":66.0,"available_capacity_mw":58.0,"fuel_type":"Natural Gas",
     "client_id":"CLI001","client_name":"Gold Fields Limited","commissioning_date":"2006-03-15","last_upgrade_date":"2019-07-20"},
    {"plant_id":"PLT002","plant_name":"Chirano Power Plant", "location":"Chirano", "region":"Ashanti Region",
     "installed_capacity_mw":33.0,"available_capacity_mw":31.0,"fuel_type":"Natural Gas",
     "client_id":"CLI002","client_name":"Kinross Gold Corporation","commissioning_date":"2015-09-01","last_upgrade_date":"2023-09-15"},
    {"plant_id":"PLT003","plant_name":"Damang Power Plant",  "location":"Damang",  "region":"Western Region",
     "installed_capacity_mw":25.5,"available_capacity_mw":25.5,"fuel_type":"Natural Gas",
     "client_id":"CLI001","client_name":"Gold Fields Limited","commissioning_date":"2010-06-10","last_upgrade_date":"2020-03-01"},
    {"plant_id":"PLT004","plant_name":"Edikan Power Plant",  "location":"Edikan",  "region":"Central Region",
     "installed_capacity_mw":33.0,"available_capacity_mw":33.0,"fuel_type":"Natural Gas",
     "client_id":"CLI003","client_name":"Perseus Mining Limited","commissioning_date":"2018-04-22","last_upgrade_date":"2022-11-10"},
    {"plant_id":"PLT005","plant_name":"Wassa Power Plant",   "location":"Wassa",   "region":"Western Region",
     "installed_capacity_mw":33.0,"available_capacity_mw":32.0,"fuel_type":"Natural Gas",
     "client_id":"CLI004","client_name":"Golden Star Resources","commissioning_date":"2016-11-05","last_upgrade_date":"2021-08-18"},
])
plants.to_csv(f"{OUT}/erp_plant_master.csv", index=False)
print(f"erp_plant_master: {len(plants)} rows")

# ── Contracts ─────────────────────────────────────────────────────────────────
contracts = pd.DataFrame([
    {"contract_id":"CTR001","client_id":"CLI001","client_name":"Gold Fields Limited",    "plant_id":"PLT001","contract_type":"PPA","contracted_capacity_mw":58.0,"tariff_usd_per_mwh":98.50, "contract_start_date":"2023-01-01","contract_end_date":"2028-12-31","billing_cycle":"Monthly","currency":"USD"},
    {"contract_id":"CTR002","client_id":"CLI001","client_name":"Gold Fields Limited",    "plant_id":"PLT003","contract_type":"PPA","contracted_capacity_mw":25.5,"tariff_usd_per_mwh":102.00,"contract_start_date":"2023-01-01","contract_end_date":"2027-12-31","billing_cycle":"Monthly","currency":"USD"},
    {"contract_id":"CTR003","client_id":"CLI002","client_name":"Kinross Gold Corporation","plant_id":"PLT002","contract_type":"PPA","contracted_capacity_mw":31.0,"tariff_usd_per_mwh":105.75,"contract_start_date":"2023-06-01","contract_end_date":"2029-05-31","billing_cycle":"Monthly","currency":"USD"},
    {"contract_id":"CTR004","client_id":"CLI003","client_name":"Perseus Mining Limited", "plant_id":"PLT004","contract_type":"PPA","contracted_capacity_mw":33.0,"tariff_usd_per_mwh":99.00, "contract_start_date":"2022-04-01","contract_end_date":"2027-03-31","billing_cycle":"Monthly","currency":"USD"},
    {"contract_id":"CTR005","client_id":"CLI004","client_name":"Golden Star Resources",  "plant_id":"PLT005","contract_type":"PPA","contracted_capacity_mw":32.0,"tariff_usd_per_mwh":101.50,"contract_start_date":"2023-01-01","contract_end_date":"2028-12-31","billing_cycle":"Monthly","currency":"USD"},
    # Historical version of CTR001 (lower tariff - shows SCD2 value)
    {"contract_id":"CTR001","client_id":"CLI001","client_name":"Gold Fields Limited",    "plant_id":"PLT001","contract_type":"PPA","contracted_capacity_mw":55.0,"tariff_usd_per_mwh":92.00, "contract_start_date":"2020-01-01","contract_end_date":"2022-12-31","billing_cycle":"Monthly","currency":"USD"},
])
contracts.to_csv(f"{OUT}/erp_contracts.csv", index=False)
print(f"erp_contracts: {len(contracts)} rows")

# ── Fuel deliveries ───────────────────────────────────────────────────────────
plant_ids = ["PLT001","PLT002","PLT003","PLT004","PLT005"]
fuel_records = []
start = datetime(2024, 1, 1)
for i in range(180):
    d = start + timedelta(days=i*2)
    for pid in plant_ids:
        vol = round(random.uniform(800, 2400), 2)
        cost = round(vol * random.uniform(3.8, 4.6), 2)
        fuel_records.append({
            "delivery_id": f"FUL{str(len(fuel_records)+1).zfill(5)}",
            "plant_id": pid,
            "delivery_date": d.strftime("%Y-%m-%d"),
            "fuel_type": "Natural Gas",
            "volume_mscf": vol,
            "unit_cost_usd_mscf": round(cost/vol, 4),
            "total_cost_usd": cost,
            "supplier": "Ghana National Gas Company",
            "pipeline_segment": "Prestea RMS" if pid in ["PLT001","PLT003","PLT005"] else "Takoradi RMS"
        })
fuel_df = pd.DataFrame(fuel_records)
fuel_df.to_csv(f"{OUT}/erp_fuel_deliveries.csv", index=False)
print(f"erp_fuel_deliveries: {len(fuel_df)} rows")

# ── Hourly plant SCADA readings ────────────────────────────────────────────────
capacities = {"PLT001":58.0,"PLT002":31.0,"PLT003":25.5,"PLT004":33.0,"PLT005":32.0}
readings = []
start = datetime(2024, 1, 1)
for day_offset in range(365):
    d = start + timedelta(days=day_offset)
    for hour in range(0, 24, 3):  # Every 3 hours = 8 readings/day/plant
        ts = d + timedelta(hours=hour)
        for pid, cap in capacities.items():
            # Simulate realistic load profiles
            hour_of_day = ts.hour
            base_load   = 0.88 + 0.1 * math.sin((hour_of_day - 6) * math.pi / 12)
            noise       = random.gauss(0, 0.03)
            load_factor = min(1.0, max(0.5, base_load + noise))

            # Random planned outage (2% chance per reading)
            if random.random() < 0.02:
                load_factor = 0.0
                status = "OFFLINE"
            elif load_factor > 0.95:
                status = "FULL_LOAD"
            elif load_factor > 0.7:
                status = "PARTIAL_LOAD"
            else:
                status = "LOW_LOAD"

            mw_output      = round(cap * load_factor, 2)
            heat_rate      = round(random.gauss(9500, 200), 0) if load_factor > 0 else 0
            fuel_consumed  = round(mw_output * heat_rate / 1e6, 4) if load_factor > 0 else 0
            efficiency_pct = round((3412 / heat_rate) * 100, 2) if heat_rate > 0 else 0.0

            readings.append({
                "reading_id":        f"RDG{str(len(readings)+1).zfill(8)}",
                "plant_id":          pid,
                "reading_timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),
                "mw_output":         mw_output,
                "fuel_consumed_mscf":fuel_consumed,
                "heat_rate_btu_kwh": heat_rate,
                "load_factor_pct":   round(load_factor * 100, 2),
                "frequency_hz":      round(random.gauss(50.0, 0.1), 2),
                "voltage_kv":        round(random.gauss(11.0, 0.15), 3),
                "status":            status
            })

readings_df = pd.DataFrame(readings)
readings_df.to_csv(f"{OUT}/scada_plant_hourly.csv", index=False)
print(f"scada_plant_hourly: {len(readings_df)} rows")

# ── Equipment alarms ──────────────────────────────────────────────────────────
alarm_codes = {
    "ALM001": ("HIGH_EXHAUST_TEMP",     "High exhaust gas temperature",     "WARNING"),
    "ALM002": ("LOW_OIL_PRESSURE",      "Low lube oil pressure",            "CRITICAL"),
    "ALM003": ("HIGH_VIBRATION",        "Excessive turbine vibration",      "WARNING"),
    "ALM004": ("FUEL_FLOW_LOW",         "Gas fuel flow below threshold",    "WARNING"),
    "ALM005": ("OVERSPEED_TRIP",        "Turbine overspeed protection trip","CRITICAL"),
    "ALM006": ("GEN_HIGH_TEMP",         "Generator high temperature",       "WARNING"),
    "ALM007": ("COMM_FAILURE",          "SCADA communication failure",      "INFO"),
    "ALM008": ("VOLTAGE_DEVIATION",     "Grid voltage deviation ±5%",       "WARNING"),
}
equip_map = {"PLT001":["TRB001","TRB002","GEN001","GEN002"],
             "PLT002":["TRB003","GEN003"],
             "PLT003":["TRB004","GEN004"],
             "PLT004":["TRB005","GEN005"],
             "PLT005":["TRB006","GEN006"]}
alarms = []
for _ in range(1800):
    pid      = random.choice(plant_ids)
    equip    = random.choice(equip_map[pid])
    code, (short, desc, sev) = random.choice(list(alarm_codes.items()))
    ts       = start + timedelta(days=random.randint(0,364), hours=random.randint(0,23), minutes=random.randint(0,59))
    duration = random.randint(2, 480) if sev != "INFO" else random.randint(1, 15)
    resolved = ts + timedelta(minutes=duration)
    alarms.append({
        "alarm_id":          f"ALM{str(len(alarms)+1).zfill(6)}",
        "plant_id":          pid,
        "equipment_id":      equip,
        "alarm_timestamp":   ts.strftime("%Y-%m-%d %H:%M:%S"),
        "alarm_code":        code,
        "alarm_short_name":  short,
        "alarm_description": desc,
        "severity":          sev,
        "duration_minutes":  duration,
        "resolved_timestamp":resolved.strftime("%Y-%m-%d %H:%M:%S"),
        "operator_id":       f"OPR{random.randint(1,12):03d}"
    })
alarms_df = pd.DataFrame(alarms)
alarms_df.to_csv(f"{OUT}/scada_equipment_alarms.csv", index=False)
print(f"scada_equipment_alarms: {len(alarms_df)} rows")

# ── Monthly invoices ──────────────────────────────────────────────────────────
contract_map = {
    "CTR001":{"client_id":"CLI001","plant_id":"PLT001","tariff":98.50,"cap_mw":58.0},
    "CTR002":{"client_id":"CLI001","plant_id":"PLT003","tariff":102.0,"cap_mw":25.5},
    "CTR003":{"client_id":"CLI002","plant_id":"PLT002","tariff":105.75,"cap_mw":31.0},
    "CTR004":{"client_id":"CLI003","plant_id":"PLT004","tariff":99.0,"cap_mw":33.0},
    "CTR005":{"client_id":"CLI004","plant_id":"PLT005","tariff":101.5,"cap_mw":32.0},
}
invoices = []
for month_offset in range(12):
    bill_date = start + timedelta(days=30*month_offset + 30)
    period    = (start + timedelta(days=30*month_offset)).strftime("%Y-%m")
    for ctr_id, info in contract_map.items():
        hrs_in_month  = 720
        avail_factor  = random.uniform(0.92, 0.99)
        energy_mwh    = round(info["cap_mw"] * hrs_in_month * avail_factor * random.uniform(0.85, 0.97), 2)
        energy_charge = round(energy_mwh * info["tariff"], 2)
        cap_charge    = round(info["cap_mw"] * 12000 / 12, 2)  # ~$12k/MW/year capacity charge
        total         = round(energy_charge + cap_charge, 2)
        paid          = random.random() > 0.05  # 95% paid
        invoices.append({
            "invoice_id":          f"INV{str(len(invoices)+1).zfill(5)}",
            "contract_id":         ctr_id,
            "client_id":           info["client_id"],
            "plant_id":            info["plant_id"],
            "billing_period":      period,
            "energy_delivered_mwh":energy_mwh,
            "capacity_charge_usd": cap_charge,
            "energy_charge_usd":   energy_charge,
            "total_invoice_usd":   total,
            "invoice_date":        bill_date.strftime("%Y-%m-%d"),
            "payment_status":      "PAID" if paid else "OUTSTANDING",
            "payment_date":        (bill_date + timedelta(days=random.randint(15,45))).strftime("%Y-%m-%d") if paid else None,
        })
invoices_df = pd.DataFrame(invoices)
invoices_df.to_csv(f"{OUT}/erp_invoices.csv", index=False)
print(f"erp_invoices: {len(invoices_df)} rows")

print("\n✅ All 6 CSV files generated")
for f in os.listdir(OUT):
    path = f"{OUT}/{f}"
    size = os.path.getsize(path)
    print(f"  {f:<35} {size:>10,} bytes")
