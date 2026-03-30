# Tableau Connection Specification

> Reference document for connecting Tableau to the Pharmacy Operations
> Analytics mart layer. Not a deployed workbook.

---

## Connection Configuration

### Snowflake (Production)
```
Server: <account>.snowflakecomputing.com
Warehouse: COMPUTE_WH
Database: PHARMA_OPS
Schema: marts
Authentication: OAuth 2.0 (preferred) or Username/Password
Role: analyst_commercial | analyst_pharmacy_ops | viewer_executive
```

### PostgreSQL (Development)
```
Port: 5432
Database: postgres
Schema: public
Authentication: Username/Password
SSL: Required
```

---

## Published Data Sources

Each mart maps to a Tableau Published Data Source for governed self-service.

### DS_Commercial_Drug_Spending
- **Table**: `marts.mart_cost_per_claim`
- **Type**: Live connection (Snowflake) or Extract (PostgreSQL)
- **Refresh**: Weekly extract refresh or live query
- **Default filters**: `drug_year = CURRENT_YEAR`, `total_beneficiaries >= 11`
- **Calculated fields**:
  - `Specialty Flag Label`: `IF [is_specialty_drug] THEN "Specialty" ELSE "Non-Specialty" END`
  - `Cost Tier`: `IF [cost_per_beneficiary_monthly] > 830 THEN "High Cost" ELSEIF > 100 THEN "Moderate" ELSE "Low Cost" END`

### DS_Prescriber_Engagement
- **Table**: `marts.mart_prescriber_engagement`
- **Type**: Extract (weekly refresh)
- **Row-level security**: Filter by `state` based on Tableau user group
- **Calculated fields**:
  - `Engagement Color`: `CASE [engagement_tier] WHEN "High" THEN "#059669" WHEN "Medium" THEN "#D97706" ELSE "#DC2626" END`

### DS_Pharmacy_Operations
- **Table**: `marts.mart_fill_volume_trends`
- **Type**: Live connection
- **Default filters**: `is_gifthealth_focus = true`

### DS_Executive_Summary
- **Table**: `marts.mart_executive_summary`
- **Type**: Extract (daily refresh)
- **Permissions**: Viewer role only (no download)

### DS_Geographic
- **Table**: `marts.mart_geographic_distribution`
- **Type**: Extract with spatial join to US state shapefile
- **Calculated fields**:
  - `Cost Per Bene Bucket`: bins for choropleth coloring

---

## Dashboard Layout Specification

### Executive Dashboard (1 page)
- **KPI bar**: Total Spending | Total Claims | Avg Cost/Claim | Specialty %
- **Line chart**: Spending trend by therapeutic area (5-year)
- **Treemap**: Therapeutic area mix by spending
- **Choropleth**: Cost per beneficiary by state
- **Filters**: Drug Year, Therapeutic Area, Specialty Flag

### Pharmacy Ops Dashboard (1 page)
- **KPI bar**: Fill Volume | Cost/Claim | Generic Rate | Day Supply
- **Bar chart**: Cost per claim by therapeutic area
- **Pareto chart**: Drug spending concentration
- **Line chart**: Brand/generic ratio trend
- **Filters**: Therapeutic Area, State, Brand/Generic

### Prescriber Dashboard (1 page)
- **Donut chart**: Engagement tier distribution
- **Bar chart**: Top specialties by claim volume
- **Scatter plot**: Claims vs brand rate by prescriber
- **Map**: Prescriber density by state
- **Filters**: Specialty, Engagement Tier, State

---

## Extract Refresh Schedule

| Data Source | Refresh | Time | Priority |
|---|---|---|---|
| DS_Executive_Summary | Daily | 6:00 AM ET | High |
| DS_Commercial_Drug_Spending | Weekly (Monday) | 5:00 AM ET | Medium |
| DS_Prescriber_Engagement | Weekly (Monday) | 5:30 AM ET | Medium |
| DS_Pharmacy_Operations | Live | N/A | N/A |
| DS_Geographic | Weekly (Monday) | 6:00 AM ET | Low |
