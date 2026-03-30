# Customer Success

Prescriber engagement, specialty adoption, and retention cohort analysis.

```sql engagement
select
  specialty,
  engagement_tier,
  count(*) as prescriber_count,
  round(avg(total_claims)) as avg_claims,
  round(avg(brand_prescribing_rate_pct), 1) as avg_brand_rate,
  round(avg(total_drug_cost), 2) as avg_drug_cost
from public_marts.mart_prescriber_engagement
group by specialty, engagement_tier
order by specialty, engagement_tier
```

```sql tier_distribution
select
  engagement_tier,
  count(*) as prescriber_count
from public_marts.mart_prescriber_engagement
group by engagement_tier
```

```sql specialty_counts
select
  specialty,
  count(*) as prescriber_count,
  sum(total_claims) as total_claims,
  round(avg(brand_prescribing_rate_pct), 1) as avg_brand_rate
from public_marts.mart_prescriber_engagement
group by specialty
order by total_claims desc
```

```sql adoption
select * from public_marts.mart_specialty_adoption_funnel
where is_gifthealth_focus = true
order by specialty_claims desc
```

```sql cohorts
select * from public_marts.mart_prescriber_retention_cohort
order by specialty, engagement_tier
```

## Prescriber Engagement Tiers

<Grid cols=3>
  <BigValue data={tier_distribution.filter(d => d.engagement_tier === 'High')} value=prescriber_count title="High Engagement" />
  <BigValue data={tier_distribution.filter(d => d.engagement_tier === 'Medium')} value=prescriber_count title="Medium Engagement" />
  <BigValue data={tier_distribution.filter(d => d.engagement_tier === 'Low')} value=prescriber_count title="Low Engagement" />
</Grid>

<BarChart
  data={tier_distribution}
  x=engagement_tier
  y=prescriber_count
  fmt=num0
  title="Prescriber Distribution by Engagement Tier"
/>

## Prescribers by Specialty

<BarChart
  data={specialty_counts}
  x=specialty
  y=total_claims
  yFmt=num0
  title="Total Claims by Prescriber Specialty"
  swapXY=true
/>

## Brand Prescribing Rate by Specialty

<BarChart
  data={specialty_counts}
  x=specialty
  y=avg_brand_rate
  title="Average Brand Prescribing Rate (%) by Specialty"
  swapXY=true
/>

## Specialty Drug Adoption (Gifthealth Focus Areas)

<DataTable data={adoption} rows=20>
  <Column id=prescriber_specialty title="Specialty" />
  <Column id=therapeutic_area title="Therapeutic Area" />
  <Column id=prescriber_count fmt=num0 title="Prescribers" />
  <Column id=specialty_adoption_rate_pct title="Adoption Rate %" />
  <Column id=specialty_claims fmt=num0 title="Specialty Claims" />
  <Column id=avg_cost_per_claim fmt=usd2 title="Avg Cost/Claim" />
</DataTable>

## Retention Cohorts by Specialty and Tier

<DataTable data={cohorts} rows=20>
  <Column id=specialty title="Specialty" />
  <Column id=engagement_tier title="Tier" />
  <Column id=census_region title="Region" />
  <Column id=prescriber_count fmt=num0 title="Prescribers" />
  <Column id=avg_claims_per_prescriber fmt=num0 title="Avg Claims" />
  <Column id=avg_brand_rate_pct title="Brand Rate %" />
</DataTable>
