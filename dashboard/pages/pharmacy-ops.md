# Pharmacy Operations

Cost per claim, fill volumes, day supply efficiency, and brand/generic dispensing ratios.

```sql fill_trends
select * from public_marts.mart_fill_volume_trends
where is_gifthealth_focus = true
order by drug_year, therapeutic_area
```

```sql cost_per_claim
select
  therapeutic_area,
  drug_year,
  count(*) as drug_count,
  round(avg(cost_per_claim), 2) as avg_cost_per_claim,
  round(avg(cost_per_beneficiary_monthly), 2) as avg_monthly_cost,
  sum(total_claims) as total_claims,
  sum(total_spending) as total_spending
from public_marts.mart_cost_per_claim
where therapeutic_area != 'Other'
group by therapeutic_area, drug_year
order by drug_year, therapeutic_area
```

```sql day_supply
select
  therapeutic_area,
  round(avg(avg_day_supply_per_claim), 1) as avg_day_supply,
  round(avg(cost_per_day_of_therapy), 2) as avg_cost_per_day,
  sum(total_claims) as total_claims
from public_marts.mart_day_supply_efficiency
where therapeutic_area != 'Other'
group by therapeutic_area
order by avg_cost_per_day desc
```

```sql brand_generic
select * from public_marts.mart_brand_generic_ratio
where is_gifthealth_focus = true
order by therapeutic_area, state
```

```sql brand_generic_summary
select
  therapeutic_area,
  sum(total_claims) as total_claims,
  round(sum(generic_claims)::numeric / nullif(sum(total_claims), 0) * 100, 1) as generic_rate,
  round(sum(brand_claims)::numeric / nullif(sum(total_claims), 0) * 100, 1) as brand_rate
from public_marts.mart_brand_generic_ratio
where is_gifthealth_focus = true
group by therapeutic_area
order by generic_rate desc
```

## Cost Per Claim by Therapeutic Area (Trend)

<LineChart
  data={cost_per_claim}
  x=drug_year
  y=avg_cost_per_claim
  series=therapeutic_area
  yFmt=usd2
  title="Average Cost Per Claim (2019-2023)"
/>

## Fill Volume Trends (Gifthealth Focus Areas)

<BarChart
  data={fill_trends.filter(d => d.drug_year === 2023)}
  x=therapeutic_area
  y=total_claims
  yFmt=num0
  title="2023 Fill Volume by Therapeutic Area"
  swapXY=true
/>

## Day Supply Efficiency

Average day supply per claim and cost per day of therapy by therapeutic area.

<BarChart
  data={day_supply}
  x=therapeutic_area
  y=avg_cost_per_day
  yFmt=usd2
  title="Average Cost Per Day of Therapy"
  swapXY=true
/>

<DataTable data={day_supply} rows=15>
  <Column id=therapeutic_area title="Therapeutic Area" />
  <Column id=avg_day_supply title="Avg Day Supply/Claim" />
  <Column id=avg_cost_per_day fmt=usd2 title="Cost/Day" />
  <Column id=total_claims fmt=num0 title="Total Claims" />
</DataTable>

## Brand vs Generic Dispensing (Gifthealth Focus)

<BarChart
  data={brand_generic_summary}
  x=therapeutic_area
  y={['generic_rate', 'brand_rate']}
  title="Generic vs Brand Dispensing Rate (%)"
  type=stacked
  swapXY=true
/>
