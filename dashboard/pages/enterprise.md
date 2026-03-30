# Enterprise Customers

Manufacturer performance reporting, therapeutic area KPIs, and geographic distribution. Simulates the partner reporting Gifthealth delivers to pharmaceutical manufacturers.

```sql ta_kpis
select * from public_marts.mart_therapeutic_area_kpis
where drug_year = 2023
order by total_spending desc
```

```sql geo
select
  state,
  sum(total_claims) as total_claims,
  sum(total_drug_cost) as total_drug_cost,
  sum(prescriber_count) as prescriber_count,
  round(sum(total_drug_cost) / nullif(sum(total_claims), 0), 2) as avg_cost_per_claim
from public_marts.mart_geographic_distribution
where is_gifthealth_focus = true
group by state
order by total_drug_cost desc
```

```sql mfr_report
select
  generic_name,
  brand_name,
  manufacturer_name,
  therapeutic_area,
  total_spending,
  total_claims,
  total_beneficiaries,
  cost_per_claim,
  cost_per_beneficiary_monthly,
  ta_spending_share_pct,
  ta_claims_share_pct
from public_marts.mart_manufacturer_performance_report
where drug_year = 2023
  and is_gifthealth_focus = true
order by total_spending desc
limit 50
```

## Therapeutic Area KPIs (2023)

<DataTable data={ta_kpis} rows=15>
  <Column id=therapeutic_area title="Therapeutic Area" />
  <Column id=drug_count title="Drugs" />
  <Column id=total_claims fmt=num0 title="Claims" />
  <Column id=total_spending fmt=usd0 title="Spending" />
  <Column id=avg_cost_per_claim fmt=usd2 title="Avg Cost/Claim" />
  <Column id=specialty_spending_pct title="Specialty %" />
  <Column id=brand_spending_pct title="Brand %" />
</DataTable>

## Geographic Distribution (Gifthealth Focus Areas)

Top states by drug cost for Gifthealth-relevant therapeutic areas.

<BarChart
  data={geo}
  x=state
  y=total_drug_cost
  yFmt=usd0
  title="Drug Cost by State (Gifthealth Focus Drugs)"
  swapXY=true
/>

<USMap
  data={geo}
  state=state
  value=avg_cost_per_claim
  title="Average Cost Per Claim by State"
  fmt=usd2
/>

## Manufacturer Performance Report (2023, Gifthealth Focus)

This table simulates the partner reporting delivered to pharmaceutical manufacturers. In production, each manufacturer would see only their own drugs filtered by RBAC.

<DataTable data={mfr_report} rows=50 search=true>
  <Column id=generic_name title="Generic Name" />
  <Column id=brand_name title="Brand Name" />
  <Column id=manufacturer_name title="Manufacturer" />
  <Column id=therapeutic_area title="Therapeutic Area" />
  <Column id=total_spending fmt=usd0 title="Spending" />
  <Column id=total_claims fmt=num0 title="Claims" />
  <Column id=cost_per_claim fmt=usd2 title="Cost/Claim" />
  <Column id=ta_spending_share_pct title="TA Share %" />
</DataTable>

<DownloadData data={mfr_report} filename="manufacturer_performance_report_2023" />
