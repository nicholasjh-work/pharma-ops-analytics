# Data Quality & Governance

Completeness monitoring, anomaly detection, and KPI governance for the analytics platform.

```sql completeness
select * from public_marts.mart_dq_completeness
order by source_model, column_name
```

```sql anomalies
select * from public_marts.mart_dq_anomalies
order by abs(cost_per_claim_zscore) desc
```

```sql anomaly_summary
select
  anomaly_status,
  count(*) as drug_count,
  round(avg(abs(cost_per_claim_zscore)), 2) as avg_zscore
from public_marts.mart_dq_anomalies
group by anomaly_status
```

## Data Completeness Scorecard

<DataTable data={completeness}>
  <Column id=source_model title="Source Model" />
  <Column id=column_name title="Column" />
  <Column id=total_rows fmt=num0 title="Total Rows" />
  <Column id=completeness_pct title="Completeness %" />
  <Column id=status title="Status" />
</DataTable>

## Anomaly Detection Summary

Drug spending anomalies detected using z-score analysis (> 2 standard deviations from therapeutic area mean).

<Grid cols=2>
  <BigValue data={anomaly_summary.filter(d => d.anomaly_status === 'Anomaly')} value=drug_count title="Anomalies (>3σ)" />
  <BigValue data={anomaly_summary.filter(d => d.anomaly_status === 'Warning')} value=drug_count title="Warnings (2-3σ)" />
</Grid>

<BarChart
  data={anomaly_summary}
  x=anomaly_status
  y=drug_count
  title="Anomaly Distribution"
/>

## Top Anomalies by Z-Score

<DataTable data={anomalies} rows=25 search=true>
  <Column id=generic_name title="Drug" />
  <Column id=brand_name title="Brand" />
  <Column id=therapeutic_area title="Therapeutic Area" />
  <Column id=cost_per_claim fmt=usd2 title="Cost/Claim" />
  <Column id=cost_per_claim_zscore title="Cost Z-Score" />
  <Column id=cost_per_beneficiary_monthly fmt=usd2 title="Monthly Cost/Bene" />
  <Column id=monthly_cost_zscore title="Monthly Z-Score" />
  <Column id=anomaly_status title="Status" />
  <Column id=total_claims fmt=num0 title="Claims" />
</DataTable>

---

## Governance Documentation

This project includes full governance documentation:

- **KPI Dictionary**: Every metric with formula, grain, exclusions, and owner. See [`docs/governance/kpi_dictionary.md`](https://github.com/nicholasjh-work/pharma-ops-analytics/blob/main/docs/governance/kpi_dictionary.md)
- **RBAC Model**: Role definitions with Snowflake, Power BI, BigQuery, and Databricks RLS implementations. See [`docs/governance/rbac_model.md`](https://github.com/nicholasjh-work/pharma-ops-analytics/blob/main/docs/governance/rbac_model.md)
- **PII Handling Policy**: Data classification, HIPAA adaptation guide, retention schedule. See [`docs/governance/pii_handling_policy.md`](https://github.com/nicholasjh-work/pharma-ops-analytics/blob/main/docs/governance/pii_handling_policy.md)
