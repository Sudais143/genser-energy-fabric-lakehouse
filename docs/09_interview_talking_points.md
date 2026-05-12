# Senior Data Engineering Interview — Talking Points
## Genser Energy Operational Intelligence Platform on Microsoft Fabric

---

## THE OPENING STORY (60 seconds — memorise this)

> "Genser Energy is an independent power producer operating five natural-gas-fired plants across Ghana — Tarkwa, Chirano, Damang, Edikan, and Wassa — serving some of the largest gold mining operations on the continent under long-term Power Purchase Agreements. Their operational data lived in two completely separate systems: a SCADA system capturing plant telemetry every few hours, and SAP ERP handling contracts and billing. Neither system talked to the other.
>
> That meant nobody could answer a question like: 'Did the Tarkwa plant meet its contracted availability in Q3?' You'd have to pull a SCADA report, pull a billing report, try to join them in Excel, and hope the plant IDs matched. I built a Microsoft Fabric lakehouse pipeline that pulls both systems into a unified medallion architecture, applies SCD Type 2 versioning to contracts and plant specs, and produces a Gold layer with hourly generation facts and monthly performance rollups. It runs at 2 AM every night. By morning, operations and finance are looking at the same numbers for the first time."

---

## WHY GENSER ENERGY SPECIFICALLY NEEDS THIS

Be ready to show you understand their business:

**The PPA billing problem:**
> "Genser's revenue is governed by Power Purchase Agreements with clients like Gold Fields and Kinross. PPAs get renegotiated — tariffs change, contracted capacity adjusts when a mine expands. If contracts are overwritten, you lose the audit trail for historical billing. SCD Type 2 on the contracts table means we can always answer 'what was the tariff in effect for the Tarkwa plant in March 2022?' That's a regulatory and commercial necessity, not a nice-to-have."

**The availability vs. capacity gap:**
> "Mining operations are 24/7. If a plant goes offline unexpectedly, the mine loses production — and Genser may face penalty clauses. The pipeline computes availability_factor_pct and contracted_delivery_pct at both hourly and monthly granularity. Operations can now see which plant is trending toward a contractual shortfall before the billing cycle closes."

**The fuel cost problem:**
> "Fuel is Genser's largest operating cost — natural gas from GNPC. The pipeline joins fuel delivery records from ERP against actual generation output from SCADA to compute fuel_cost_per_mwh. That's the margin metric. You can't track it if your systems don't talk."

---

## DESIGN DECISIONS — defend each one

### Why SCD Type 2 on contracts AND plant_master?

> "Contracts are obvious — tariff renegotiations must be preserved for billing audits. Plant master is less obvious, but critical: Genser de-rates its plants after major turbine inspections — available capacity drops temporarily. If we overwrite that, we can't tell whether a plant was operating within or outside its rated capacity during a specific period. SCD2 on plant_master gives us full history of plant specs, which is essential for contractual compliance reporting."

### Why two separate Gold fact tables (hourly + monthly)?

> "Different consumers need different granularities. The control room wants hourly — they're watching heat rate deviations and real-time availability. Finance wants monthly — they're reconciling energy delivered against invoices and checking payment status. Building both from the same Silver source means a single version of truth, just served at different grain sizes. No divergence possible."

### Why is heat_rate_btu_kwh important?

> "Heat rate is the efficiency metric for gas turbines — how many BTUs of fuel you burn per kWh of electricity produced. A rising heat rate means declining efficiency: you're spending more on gas to produce the same output. Flagging heat rate values outside the physical bounds (7,000–15,000 BTU/kWh) catches both sensor errors and genuine turbine degradation. For Genser, a 5% heat rate increase across five plants translates to millions in additional fuel cost per year."

### Why incremental ingestion for SCADA data?

> "SCADA records every 3 hours across 5 plants — that's 14,600 readings per year. Manageable now, but if readings become hourly or per-minute, a full reload every night becomes expensive. Incremental ingestion is bounded by what's new, not what exists. The watermark is the reading_timestamp, which SCADA guarantees is monotonically increasing. First run loads everything; every subsequent run loads only new readings."

### Why did you add a data quality quarantine table?

> "The original design didn't have one. I added dq_quarantine in the Silver layer because energy data has specific failure modes: SCADA sensors go offline and emit null readings, ERP data entry errors create negative contract values, plant IDs get prefixed differently between systems. Silently dropping bad rows masks data quality problems. The quarantine table captures every rejected row with the reason and source — so a data quality issue surfaces as a growing quarantine count, not as a silent gap in a dashboard."

---

## MICROSOFT FABRIC — show platform knowledge

### Why Fabric over Databricks for Genser?

> "Genser's end consumers are operations managers and finance controllers — they live in Power BI. In Databricks, you'd need a SQL endpoint, a dataset connection, a gateway. In Fabric, the Gold Lakehouse IS the semantic model. You connect Power BI, add the three relationships, and the dashboard is live. For a 200-person company that doesn't have a dedicated BI team, that operational simplicity matters. The platform choice matches the organisation."

### How do the two Gold tables connect in Power BI?

> "fact_generation[plant_key] → dim_plants[plant_key] for the hourly table. fact_monthly_performance[plant_id] → dim_plants[plant_id] for the monthly rollup. dim_contracts is independent — it supports ad-hoc contract history queries. The star schema is clean."

### What Fabric features are you using beyond notebooks?

> "Data Factory pipeline for orchestration — 6 notebooks with dependency wiring so Silver runs in parallel after Bronze, and Gold waits for Silver to complete. Scheduled trigger at 2 AM. OneLake for unified storage — all three Lakehouses write to the same underlying storage, so there's no data copying between layers."

---

## FAILURE HANDLING — the senior question

**Q: What happens if SCADA goes down for 6 hours and we miss readings?**
> "The SCADA system will buffer readings locally. When connectivity is restored, they'll flush to the CSV export. Our incremental watermark picks them up on the next run — even if they arrive late, they'll be loaded because we filter on reading_timestamp, not on file arrival time. No data loss."

**Q: What if the Silver SCD2 merge partially fails?**
> "The close-old-record and insert-new-record operations are two separate steps right now. If the insert fails after the close, we have records with closed effective_end_date but no replacement. The validation step catches this — it checks that every plant and contract has exactly one is_current == True record. For production, I'd wrap both operations in a Delta transaction to guarantee atomicity."

**Q: What if a contract gets backdated in SAP?**
> "That's a genuine gap in my current design. The SCD2 logic creates a new version with today as the effective_start_date. If a contract amendment is backdated to six months ago, my effective dates won't match the actual business dates. The fix is to use the contract's own start/end dates as the SCD2 window rather than the pipeline run date. I'd implement that before going to production."

---

## SCALE — the architect question

**Q: Genser plans to add 700MW of wind power. How does this scale?**
> "Wind adds complexity: weather-dependent generation, different SCADA telemetry (wind speed, blade pitch, turbine RPM), different fuel cost model (no fuel). I'd add a new source type — scada_wind_turbine — with its own Bronze/Silver path. The Gold layer stays the same: fact_generation absorbs any plant type. The dim_plants dimension gets a fuel_type column — 'Natural Gas' vs 'Wind'. All existing Power BI reports continue working. Adding a new energy source is additive, not a redesign."

---

## QUICK NUMBERS — know these cold

- **5** operational plants (Tarkwa 58MW, Chirano 31MW, Damang 25.5MW, Edikan 33MW, Wassa 32MW)
- **200MW+** total installed capacity
- **425km** natural gas pipeline network
- **4** major mining clients (Gold Fields, Kinross, Perseus Mining, Golden Star Resources)
- **6** Bronze source tables (2 SCADA, 4 ERP)
- **6** Silver tables (2 SCD Type 2, 2 incremental, 2 overwrite/incremental)
- **4** Gold tables (dim_plants, dim_contracts, fact_generation, fact_monthly_performance)
- **14,600** SCADA readings per year in demo dataset
- **2 AM** scheduled pipeline run
- **Key metrics**: Availability Factor, Capacity Factor, Heat Rate, Fuel Cost/MWh, Contracted Delivery %

---

## QUESTIONS TO ASK GENSER

These show you've done your research:

1. *"Your website mentions a 700MW wind project in the Eastern Region — is that data infrastructure already being planned, or would this pipeline be the foundation for that?"*
2. *"Do your PPAs include penalty clauses for availability shortfalls? Understanding the financial stakes would help me prioritise which data quality rules matter most."*
3. *"Is SCADA data currently exported manually to CSV, or is there an API or real-time stream we could tap for near-real-time ingestion?"*
4. *"How is the current relationship between operations and finance when it comes to data — do they use the same reports, or are there currently two competing versions of plant performance?"*

---

## THE CLOSING LINE

If they ask "why did you build this for Genser specifically?":

> "Because the problem is real. An IPP like Genser is operationally complex — five plants, long-term contracts, a gas pipeline, clients with 24/7 demand. When your revenue depends on delivering contracted megawatts and your systems don't talk to each other, you're flying blind on your most important KPIs. I wanted to build something that actually solves that, not just something that looks good in a portfolio."
