# About This Project

## Overview

This analytics platform is built on **real CMS Medicare Part D public use files**, not synthetic data. It demonstrates the full data architecture, engineering, governance, and BI delivery patterns used in pharmacy and healthcare analytics.

The platform processes **26.8 million** prescription drug event records, filters to pharmacy-relevant therapeutic areas, and delivers governed analytics across four business unit domains.

## Architecture

```
CMS Public Data (CSV, 4.2 GB)
    → Python ingestion (download, validate, filter)
    → PostgreSQL / Snowflake / BigQuery / Databricks
    → dbt (staging → intermediate → marts)
    → Evidence dashboard (this site)
```

## Data Sources

| Source | Rows | Description |
|---|---|---|
| Part D Spending by Drug | 60,478 | Drug-level spending, 2019-2023 (5 years unpivoted) |
| Prescribers by Provider | 676,186 | Per-prescriber summary, 11 target specialties |
| Prescribers by Drug | 5,451,672 | Prescriber-drug detail, filtered to target therapeutic areas |

## dbt Project

**23 models** across 4 layers, **48 tests** (47 pass, 1 warn), **1 SCD Type 2 snapshot**.

Mart domains map to business units:

| Domain | Models | Key KPIs |
|---|---|---|
| Commercial | 3 | Spending trends, therapeutic area mix, Pareto analysis |
| Customer Success | 3 | Engagement tiers, specialty adoption, retention cohorts |
| Pharmacy Operations | 4 | Cost/claim, fill volumes, day supply, brand/generic ratio |
| Enterprise Customers | 3 | Manufacturer performance, TA KPIs, geographic distribution |
| Executive | 2 | KPI scorecard, cross-BU summary |
| Data Quality | 2 | Completeness scorecard, anomaly detection |

## Governance

- **KPI Dictionary**: 16 metrics with formula, grain, exclusions, and owner
- **RBAC Model**: 6 roles with Snowflake, Power BI, BigQuery, and Databricks RLS
- **PII Handling Policy**: HIPAA adaptation guide, retention schedule

## BI Tool Integration

- **LookML model** with explores, views, measures, and access filters
- **Tableau connection spec** with published data sources and extract schedule
- **Evidence dashboard** (this site) querying dbt marts directly

## Source Code

[github.com/nicholasjh-work/pharma-ops-analytics](https://github.com/nicholasjh-work/pharma-ops-analytics)

---

Built by [Nicholas Hidalgo](https://nicholashidalgo.com) | [LinkedIn](https://linkedin.com/in/nicholashidalgo) | analytics@nicholashidalgo.com
