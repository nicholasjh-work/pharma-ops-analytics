# Pharmacy Operations Analytics Platform

Real CMS Medicare Part D data powering pharmacy operations analytics across four business units.

```sql kpi_scorecard
select * from public_marts.mart_kpi_scorecard
order by drug_year
```

```sql executive_summary
select * from public_marts.mart_executive_summary
order by drug_year, total_spending desc
```

```sql spending_trends
select * from public_marts.mart_drug_spending_trends
order by drug_year, therapeutic_area
```

```sql anomalies
select * from public_marts.mart_dq_anomalies
where anomaly_status = 'Anomaly'
order by abs(cost_per_claim_zscore) desc
limit 20
```

## KPI Scorecard

<Grid cols=4>
  <BigValue data={kpi_scorecard.filter(d => d.drug_year === 2023)} value=total_spending title="Total Spending (2023)" fmt=usd0 />
  <BigValue data={kpi_scorecard.filter(d => d.drug_year === 2023)} value=total_claims title="Total Claims (2023)" fmt=num0 />
  <BigValue data={kpi_scorecard.filter(d => d.drug_year === 2023)} value=avg_cost_per_claim title="Avg Cost/Claim" fmt=usd2 />
  <BigValue data={kpi_scorecard.filter(d => d.drug_year === 2023)} value=specialty_spending_pct title="Specialty %" fmt=pct1 />
</Grid>

## Spending Trends by Year

<LineChart
  data={kpi_scorecard}
  x=drug_year
  y=total_spending
  yFmt=usd0
  title="Total Part D Spending (2019-2023)"
/>

## Spending by Therapeutic Area

<BarChart
  data={executive_summary.filter(d => d.drug_year === 2023)}
  x=therapeutic_area
  y=total_spending
  yFmt=usd0
  title="2023 Spending by Therapeutic Area"
  sort=false
  swapXY=true
/>

## Therapeutic Area Trends (5-Year)

<LineChart
  data={spending_trends}
  x=drug_year
  y=total_spending
  series=therapeutic_area
  yFmt=usd0
  title="Spending Trends by Therapeutic Area"
/>

## Top Anomalies Detected

<DataTable data={anomalies} rows=10>
  <Column id=generic_name title="Drug" />
  <Column id=therapeutic_area title="Therapeutic Area" />
  <Column id=cost_per_claim fmt=usd2 title="Cost/Claim" />
  <Column id=cost_per_claim_zscore title="Z-Score" />
  <Column id=anomaly_status title="Status" />
</DataTable>

---

<Grid cols=3>
  <BigLink href="/commercial">Commercial Analytics</BigLink>
  <BigLink href="/customer-success">Customer Success</BigLink>
  <BigLink href="/pharmacy-ops">Pharmacy Operations</BigLink>
</Grid>
<Grid cols=3>
  <BigLink href="/enterprise">Enterprise Customers</BigLink>
  <BigLink href="/data-quality">Data Quality</BigLink>
  <BigLink href="/about">About This Project</BigLink>
</Grid>
