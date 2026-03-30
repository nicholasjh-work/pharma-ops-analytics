# Commercial Analytics

Drug spending trends, therapeutic area market sizing, and specialty drug concentration.

```sql spending_trends
select * from public_marts.mart_drug_spending_trends
order by drug_year, therapeutic_area
```

```sql ta_mix
select * from public_marts.mart_therapeutic_area_mix
order by drug_year, total_spending desc
```

```sql specialty
select * from public_marts.mart_specialty_vs_generic
where drug_year = 2023
order by spending_rank
limit 50
```

```sql top_drugs
select
  generic_name,
  brand_name,
  manufacturer_name,
  therapeutic_area,
  total_spending,
  total_claims,
  cost_per_claim,
  cost_per_beneficiary_monthly,
  is_specialty_drug
from public_marts.mart_cost_per_claim
where drug_year = 2023
order by total_spending desc
limit 25
```

## Spending by Therapeutic Area (5-Year Trend)

<LineChart
  data={spending_trends}
  x=drug_year
  y=total_spending
  series=therapeutic_area
  yFmt=usd0
  title="Spending Trends by Therapeutic Area"
/>

## Market Mix: Brand vs Generic

<BarChart
  data={ta_mix.filter(d => d.drug_year === 2023)}
  x=therapeutic_area
  y=total_spending
  series=brand_generic_flag
  yFmt=usd0
  title="2023 Spending: Brand vs Generic by Therapeutic Area"
  type=stacked
  swapXY=true
/>

## Specialty Drug Concentration (Pareto)

The top 20% of drugs account for the vast majority of spending. This Pareto analysis shows cumulative spending concentration.

<LineChart
  data={specialty}
  x=spending_rank
  y=cumulative_spending_pct
  yFmt=pct1
  title="Cumulative Spending by Drug Rank (2023)"
  yMax=100
/>

## Top 25 Drugs by Spending (2023)

<DataTable data={top_drugs} rows=25>
  <Column id=generic_name title="Generic Name" />
  <Column id=brand_name title="Brand Name" />
  <Column id=manufacturer_name title="Manufacturer" />
  <Column id=therapeutic_area title="Therapeutic Area" />
  <Column id=total_spending fmt=usd0 title="Total Spending" />
  <Column id=total_claims fmt=num0 title="Claims" />
  <Column id=cost_per_claim fmt=usd2 title="Cost/Claim" />
  <Column id=is_specialty_drug title="Specialty" />
</DataTable>
