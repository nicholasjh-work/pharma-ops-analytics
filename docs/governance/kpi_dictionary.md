# KPI Dictionary — Pharmacy Operations Analytics Platform

> Governed metric definitions with formula, grain, exclusions, and ownership.
> This dictionary is the single source of truth for all metrics surfaced in
> dashboards, reports, and partner deliverables.

---

## Cost Metrics

### Cost Per Claim
- **Formula**: `total_drug_cost / total_claims`
- **Grain**: drug + year
- **Source model**: `mart_cost_per_claim`
- **Exclusions**: Suppressed cells (< 11 beneficiaries per CMS policy)
- **Owner**: Data & Analytics
- **Refresh**: Annual (aligned to CMS release cycle)
- **Notes**: Represents gross drug cost including Medicare, plan, and beneficiary payments. Does not reflect manufacturer rebates.

### Cost Per Beneficiary Per Month
- **Formula**: `(total_drug_cost / total_beneficiaries) / 12`
- **Grain**: drug + year
- **Source model**: `mart_cost_per_claim`
- **Exclusions**: Suppressed cells (< 11 beneficiaries)
- **Owner**: Data & Analytics
- **Notes**: Used to determine CMS specialty drug classification ($830/month threshold).

### Cost Per Day of Therapy
- **Formula**: `total_drug_cost / total_day_supply`
- **Grain**: drug + state + year
- **Source model**: `mart_day_supply_efficiency`
- **Exclusions**: Records with zero day supply
- **Owner**: Pharmacy Operations

---

## Volume Metrics

### Total Claims
- **Formula**: `sum(total_claims)` at the relevant grain
- **Grain**: varies by mart (drug, prescriber, therapeutic area, state)
- **Source models**: All mart models
- **Exclusions**: None
- **Owner**: Data & Analytics

### Fills Per Beneficiary
- **Formula**: `total_30day_fills / total_beneficiaries`
- **Grain**: therapeutic_area + drug_year
- **Source model**: `mart_fill_volume_trends`
- **Exclusions**: Suppressed cells
- **Owner**: Pharmacy Operations
- **Notes**: Proxy for medication adherence. Higher values suggest better fill continuity.

---

## Classification Metrics

### Specialty Drug Flag
- **Formula**: `cost_per_beneficiary_month > $830`
- **Grain**: drug + year
- **Source model**: `mart_cost_per_claim`, `int_drug_enriched`
- **Exclusions**: None
- **Owner**: Data & Analytics
- **Notes**: Uses CMS standard specialty drug definition. Seed mapping provides manual overrides for known specialty drugs.

### Generic Dispensing Rate
- **Formula**: `generic_claims / (brand_claims + generic_claims) * 100`
- **Grain**: therapeutic_area + state + year
- **Source model**: `mart_brand_generic_ratio`
- **Exclusions**: Prescribers with zero total claims
- **Owner**: Pharmacy Operations

### Brand Prescribing Rate
- **Formula**: `brand_claims / total_claims * 100`
- **Grain**: prescriber (NPI) + year
- **Source model**: `mart_prescriber_engagement`
- **Exclusions**: None
- **Owner**: Customer Success

---

## Market Metrics

### Therapeutic Area Spending Share
- **Formula**: `ta_spending / total_spending * 100`
- **Grain**: therapeutic_area + year
- **Source model**: `mart_therapeutic_area_mix`, `mart_executive_summary`
- **Exclusions**: None
- **Owner**: Commercial

### Market Share (Spending)
- **Formula**: `drug_spending / ta_total_spending * 100`
- **Grain**: drug + therapeutic_area + year
- **Source model**: `mart_manufacturer_performance_report`
- **Exclusions**: Suppressed cells
- **Owner**: Enterprise Customers

---

## Engagement Metrics

### Prescriber Engagement Tier
- **Formula**: `CASE WHEN total_claims >= P75 THEN 'High' WHEN >= P25 THEN 'Medium' ELSE 'Low' END`
- **Grain**: prescriber (NPI) + year
- **Source model**: `mart_prescriber_engagement`
- **Exclusions**: Prescribers with zero claims
- **Owner**: Customer Success
- **Notes**: Percentiles calculated across all prescribers in the dataset for the given year.

### Specialty Adoption Rate
- **Formula**: `prescribers_with_specialty_rx / total_prescriber_count * 100`
- **Grain**: prescriber_specialty + therapeutic_area + year
- **Source model**: `mart_specialty_adoption_funnel`
- **Exclusions**: None
- **Owner**: Customer Success

---

## Data Quality Metrics

### Data Completeness
- **Formula**: `count(non_null_values) / count(*) * 100`
- **Grain**: source_model + column_name
- **Source model**: `mart_dq_completeness`
- **Thresholds**: Pass >= 99%, Warning >= 95%, Fail < 95%
- **Owner**: Data & Analytics

### Spending Anomaly (Z-Score)
- **Formula**: `(value - mean) / stddev` per therapeutic area
- **Grain**: drug + year
- **Source model**: `mart_dq_anomalies`
- **Thresholds**: Normal < 2, Warning 2-3, Anomaly > 3
- **Owner**: Data & Analytics
