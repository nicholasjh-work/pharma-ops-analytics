<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="assets/nh-banner-dark.png">
    <source media="(prefers-color-scheme: light)" srcset="assets/nh-banner-light.png">
    <img src="assets/nh-banner-dark.png" alt="Pharmacy Operations Analytics Platform Banner" width="100%">
  </picture>
</p>

<h1 align="center">Pharmacy Operations Analytics Platform</h1>

<p align="center">
    <b>Real CMS Medicare Part D data powering a pharmacy operations analytics stack with dbt, PostgreSQL, and React</b>
</p>

<p align="center">
    <a href="https://pharma-ops.nicholashidalgo.com"><img src="https://img.shields.io/badge/Demo-Live_Dashboard-0F766E?style=for-the-badge" alt="Demo"></a>&nbsp;
    <a href="LICENSE"><img src="https://img.shields.io/badge/License-MIT-1E40AF?style=for-the-badge" alt="License"></a>
</p>

<p align="center">
    <img src="https://img.shields.io/badge/Python-3.11-3776AB?style=flat&logo=python&logoColor=white" alt="Python">
    <img src="https://img.shields.io/badge/dbt-Core-FF694B?style=flat&logo=dbt&logoColor=white" alt="dbt">
    <!-- add remaining tech badges here -->
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.11-3776AB?style=flat&logo=python&logoColor=white" alt="Python">
  <img src="https://img.shields.io/badge/dbt-Core-FF694B?style=flat&logo=dbt&logoColor=white" alt="dbt">
  <img src="https://img.shields.io/badge/PostgreSQL-16-4169E1?style=flat&logo=postgresql&logoColor=white" alt="PostgreSQL">
  <img src="https://img.shields.io/badge/Snowflake-Ready-29B5E8?style=flat&logo=snowflake&logoColor=white" alt="Snowflake">
  <img src="https://img.shields.io/badge/React-18-61DAFB?style=flat&logo=react&logoColor=black" alt="React">
  <img src="https://img.shields.io/badge/TypeScript-5-3178C6?style=flat&logo=typescript&logoColor=white" alt="TypeScript">
  <img src="https://img.shields.io/badge/Tailwind-3-06B6D4?style=flat&logo=tailwindcss&logoColor=white" alt="Tailwind">
</p>

---

### Overview

An end-to-end analytics platform built on real CMS Medicare Part D public use files. Demonstrates data architecture, engineering, governance, and BI delivery patterns used in pharmacy and healthcare analytics.

The platform processes 25M+ prescription drug event records, filters to pharmacy-relevant therapeutic areas (gastroenterology, immunology, weight management, oncology), and delivers governed analytics across four business unit domains: Commercial, Customer Success, Pharmacy Operations, and Enterprise Customers.

### Architecture

```
CMS Public Data (CSV)
    │
    ▼
Python Ingestion (scripts/ingest.py)
    │  Download, validate, filter, load
    ▼
Data Warehouse (PostgreSQL · Snowflake · BigQuery · Databricks)
    │
    ▼
dbt (staging → intermediate → marts)
    │  20+ models, tests, snapshots, docs
    ▼
React Dashboard (6 pages)
    │  Recharts, REST API
    ▼
Vercel + Cloudflare (pharma-ops.nicholashidalgo.com)
```

### Data Sources

All data is real and publicly available from CMS (data.cms.gov). No synthetic or dummy data.

- **Medicare Part D Spending by Drug (2023)** — drug-level spending, claims, and beneficiary counts (~600 rows)
- **Medicare Part D Prescribers by Provider (2023)** — per-prescriber summary with specialty, state, brand/generic splits (~1.1M rows, filtered to target specialties)
- **Medicare Part D Prescribers by Provider and Drug (2023)** — prescriber-drug detail (~25M rows, filtered to target therapeutic areas via `seed_therapeutic_area_map.csv`)

### dbt Project

**20 models** across 4 layers:

| Layer | Models | Materialization |
|---|---|---|
| Staging | 3 (drug spending, prescribers, prescriber-drugs) | View |
| Intermediate | 3 (drug enriched, prescriber summary, prescriber-drug detail) | Table |
| Marts | 15 across 6 domains | Table |
| Snapshots | 1 (SCD Type 2 drug pricing) | Snapshot |

**Mart domains** map to business units:

| Domain | Models | Key KPIs |
|---|---|---|
| Commercial | 3 | Spending trends, therapeutic area mix, Pareto analysis |
| Customer Success | 3 | Prescriber engagement tiers, specialty adoption, retention cohorts |
| Pharmacy Operations | 4 | Cost/claim, fill volumes, day supply efficiency, brand/generic ratio |
| Enterprise Customers | 3 | Manufacturer performance, TA KPIs, geographic distribution |
| Executive | 2 | KPI scorecard, cross-BU executive summary |
| Data Quality | 2 | Completeness scorecard, anomaly detection (z-score) |

### Governance

- **KPI Dictionary** (`docs/governance/kpi_dictionary.md`) — every metric with formula, grain, exclusions, and owner
- **RBAC Model** (`docs/governance/rbac_model.md`) — role definitions with Snowflake, Power BI, BigQuery, and Databricks RLS examples
- **PII Handling Policy** (`docs/governance/pii_handling_policy.md`) — data classification, HIPAA adaptation guide, retention schedule

### BI Tool Integration

- **LookML Model** (`docs/bi_connections/lookml_model.lkml`) — explores, views, measures, and row-level access filters
- **Tableau Spec** (`docs/bi_connections/tableau_connection_spec.md`) — published data sources, calculated fields, extract schedule

### Dashboard

Six pages covering all business unit domains:

1. **Executive Summary** — KPI scorecard, spending trends, geographic heatmap
2. **Commercial** — therapeutic area market sizing, manufacturer share, specialty concentration
3. **Customer Success** — prescriber engagement, specialty adoption, retention cohorts
4. **Pharmacy Operations** — cost/claim trends, day supply efficiency, brand/generic ratios
5. **Enterprise** — manufacturer performance reports, exportable partner data
6. **Data Quality** — completeness, freshness, anomaly alerts

### Getting Started

```bash
# Clone
git clone https://github.com/nicholasjh-work/pharma-ops-analytics.git
cd pharma-ops-analytics

# Python environment
python -m venv .venv
source .venv/bin/activate
pip install pandas sqlalchemy psycopg2-binary python-dotenv

# Configure
cp .env.example .env
# Edit .env with your database credentials

# Download CMS data (~2 GB total, cached after first run)
python scripts/ingest.py --download-only

# Load into your target warehouse
python scripts/ingest.py --target local        # PostgreSQL
python scripts/ingest.py --target snowflake    # Snowflake
python scripts/ingest.py --target bigquery     # BigQuery
python scripts/ingest.py --target databricks   # Databricks

# dbt (install the adapter for your target)
pip install dbt-postgres     # or dbt-snowflake, dbt-bigquery, dbt-databricks
cp profiles.yml.example ~/.dbt/profiles.yml
# Edit profiles.yml: set target to your warehouse
dbt deps
dbt seed
dbt run
dbt test
dbt docs generate && dbt docs serve
```

### Supported Data Warehouses

This project runs on four warehouse targets with the same dbt models. See [`docs/platform_notes.md`](docs/platform_notes.md) for detailed setup, bulk loading, and SQL compatibility notes per platform.

| Platform | Adapter | Free Tier | Best For |
|---|---|---|---|
| **PostgreSQL** | dbt-postgres | Local (free) | Development, testing |
| **Snowflake** | dbt-snowflake | 30-day trial ($400 credit) | Production analytics, warehouse sizing demos |
| **BigQuery** | dbt-bigquery | 1 TB/month queries, 10 GB storage | Cost-per-query workloads, GCP integration |
| **Databricks** | dbt-databricks | Community Edition (permanent) | Unity Catalog, Delta Lake, notebook workflows |

### Project Structure

```
pharma-ops-analytics/
├── scripts/ingest.py              # CMS data download, filter, and load
├── seeds/                         # Reference data (therapeutic areas, regions)
├── models/
│   ├── staging/                   # 1:1 with raw sources, type casting, cleanup
│   ├── intermediate/              # Joins, enrichment, derived fields
│   └── marts/                     # Business-ready models by domain
│       ├── commercial/
│       ├── customer_success/
│       ├── pharmacy_ops/
│       ├── enterprise/
│       ├── executive/
│       └── data_quality/
├── snapshots/                     # SCD Type 2 for drug pricing
├── tests/                         # Custom data quality assertions
├── macros/                        # Reusable SQL (small-cell suppression)
├── docs/
│   ├── governance/                # KPI dictionary, RBAC, PII policy
│   ├── bi_connections/            # LookML model, Tableau spec
│   └── platform_notes.md         # Snowflake, BigQuery, Databricks setup
├── dashboard/                     # React + TypeScript frontend
└── assets/                        # NH logo (dark/light SVG)
```

---

<p align="center">
  <a href="https://linkedin.com/in/nicholashidalgo"><img src="https://img.shields.io/badge/LinkedIn-Nicholas_Hidalgo-0A66C2?style=for-the-badge&logo=linkedin" alt="LinkedIn"></a>&nbsp;
  <a href="https://nicholashidalgo.com"><img src="https://img.shields.io/badge/Website-nicholashidalgo.com-0F766E?style=for-the-badge" alt="Website"></a>&nbsp;
  <a href="mailto:analytics@nicholashidalgo.com"><img src="https://img.shields.io/badge/Email-analytics@nicholashidalgo.com-1E40AF?style=for-the-badge" alt="Email"></a>
</p>
