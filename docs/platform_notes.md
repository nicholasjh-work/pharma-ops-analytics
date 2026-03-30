# Platform Deployment Notes

> Instructions for running this project on each supported data warehouse.
> All dbt models are cross-compatible. Platform-specific adjustments are
> documented below.

---

## Local PostgreSQL (Development)

**Setup time**: 5 minutes

```bash
# Create database
createdb pharma_ops

# Install dbt adapter
pip install dbt-postgres

# Configure
cp profiles.yml.example ~/.dbt/profiles.yml
# Set target: dev_postgres (default)

# Load data and run
python scripts/ingest.py --target local
dbt deps && dbt seed && dbt run && dbt test
```

**Known considerations**:
- `array_agg` with `ORDER BY` works natively
- `percentile_cont` works natively
- No warehouse sizing concerns (local resources)

---

## Snowflake

**Setup time**: 15 minutes
**Cost**: Free trial ($400 credit, 30 days) at https://signup.snowflake.com

### Initial Setup

```sql
-- Run in Snowflake worksheet after trial activation
CREATE DATABASE PHARMA_OPS;
CREATE SCHEMA PHARMA_OPS.PUBLIC;
CREATE WAREHOUSE COMPUTE_WH
    WITH WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE;
```

### Load Data

```bash
# Option A: Via Python (small/medium datasets)
pip install snowflake-sqlalchemy snowflake-connector-python
python scripts/ingest.py --target snowflake

# Option B: Via Snowflake stage (recommended for 25M+ rows)
# Upload CSVs to an internal stage, then COPY INTO
```

**Bulk loading via stage** (for the full 25M-row prescriber-drug file):

```sql
-- Create stage
CREATE OR REPLACE STAGE pharma_ops_stage;

-- Upload from SnowSQL CLI
PUT file:///path/to/data/processed/prescribers_drug_processed.csv @pharma_ops_stage;

-- Load
COPY INTO raw_prescribers_drug
    FROM @pharma_ops_stage/prescribers_drug_processed.csv.gz
    FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');
```

### Run dbt

```bash
pip install dbt-snowflake

# Edit ~/.dbt/profiles.yml: set target to 'snowflake'
dbt deps && dbt seed && dbt run && dbt test
```

### Performance Tuning

```sql
-- Cluster high-traffic mart tables
ALTER TABLE marts.mart_cost_per_claim
    CLUSTER BY (drug_year, therapeutic_area);

ALTER TABLE marts.mart_geographic_distribution
    CLUSTER BY (drug_year, state);

-- Use XSMALL for dashboard queries, MEDIUM for full rebuild
ALTER WAREHOUSE COMPUTE_WH SET WAREHOUSE_SIZE = 'XSMALL';
```

### SQL Adjustments

| PostgreSQL | Snowflake Equivalent |
|---|---|
| `current_timestamp` | `current_timestamp()` |
| `(array_agg(x ORDER BY y))[1]` | `ARRAY_AGG(x) WITHIN GROUP (ORDER BY y)[0]` |
| `::numeric` | `::NUMBER` |
| `filter (where ...)` | Use `CASE WHEN ... THEN ... END` inside aggregate |

These adjustments are minor. The dbt models use `dbt_utils` functions that
handle most cross-platform differences automatically.

---

## Google BigQuery

**Setup time**: 20 minutes
**Cost**: Free tier (1 TB/month queries, 10 GB storage, 10 GB/month streaming inserts)

### Initial Setup

1. Go to https://console.cloud.google.com
2. Create a new project (e.g., `pharma-ops-analytics`)
3. Enable the BigQuery API
4. Create a service account:
   - IAM & Admin > Service Accounts > Create
   - Role: BigQuery Admin
   - Create key (JSON) and save to `~/.gcp/pharma-ops-sa.json`
5. Create dataset:

```sql
-- In BigQuery console
CREATE SCHEMA pharma_ops;
```

### Load Data

```bash
# Option A: Via Python
pip install sqlalchemy-bigquery google-auth
python scripts/ingest.py --target bigquery

# Option B: Via bq CLI (faster for large files)
bq load --source_format=CSV --autodetect \
    pharma_ops.raw_part_d_spending \
    data/processed/spending_processed.csv

bq load --source_format=CSV --autodetect \
    pharma_ops.raw_prescribers \
    data/processed/prescribers_processed.csv

bq load --source_format=CSV --autodetect \
    pharma_ops.raw_prescribers_drug \
    data/processed/prescribers_drug_processed.csv
```

### Run dbt

```bash
pip install dbt-bigquery

# Edit ~/.dbt/profiles.yml: set target to 'bigquery'
dbt deps && dbt seed && dbt run && dbt test
```

### SQL Adjustments

| PostgreSQL | BigQuery Equivalent |
|---|---|
| `serial` / `bigserial` | `INT64` (no auto-increment, use `GENERATE_UUID()`) |
| `varchar(n)` | `STRING` |
| `numeric(18,2)` | `NUMERIC` or `BIGNUMERIC` |
| `percentile_cont(0.75) within group (order by x)` | `PERCENTILE_CONT(x, 0.75) OVER()` |
| `current_timestamp` | `CURRENT_TIMESTAMP()` |
| `::numeric` | `CAST(x AS NUMERIC)` |

### Cost Management
- Partitioned tables reduce scan cost: partition mart tables by `drug_year`
- Use `--require-partition-filter` for large tables
- Monitor with BigQuery Slot Estimator

---

## Databricks

**Setup time**: 20 minutes
**Cost**: Community Edition (free, permanent) or workspace trial (14 days)

### Initial Setup (Community Edition)

1. Sign up at https://community.cloud.databricks.com
2. Create a cluster (Community Edition provides a single shared cluster)
3. Note: Community Edition does not support SQL Warehouses or Unity Catalog.
   For full SQL Warehouse + Unity Catalog experience, use a workspace trial.

### Initial Setup (Workspace Trial)

1. Sign up at https://www.databricks.com/try-databricks
2. Create a SQL Warehouse:
   - SQL > SQL Warehouses > Create
   - Size: 2X-Small (cheapest)
   - Auto stop: 10 minutes
3. Create catalog and schema:

```sql
CREATE CATALOG IF NOT EXISTS main;
CREATE SCHEMA IF NOT EXISTS main.pharma_ops;
```

4. Generate a personal access token:
   - Settings > Developer > Access tokens > Generate new token

5. Note the HTTP path from your SQL Warehouse:
   - SQL Warehouses > your warehouse > Connection details > HTTP path

### Load Data

```bash
# Option A: Via Python
pip install databricks-sql-connector sqlalchemy-databricks
python scripts/ingest.py --target databricks

# Option B: Via Databricks UI
# Upload CSVs to Unity Catalog volumes, then COPY INTO

# Option C: Via Databricks CLI
databricks fs cp data/processed/spending_processed.csv dbfs:/pharma_ops/
```

**Bulk loading via Unity Catalog volumes** (workspace trial only):

```sql
-- Create volume for data staging
CREATE VOLUME IF NOT EXISTS main.pharma_ops.raw_data;

-- After uploading CSVs to the volume:
COPY INTO main.pharma_ops.raw_part_d_spending
    FROM '/Volumes/main/pharma_ops/raw_data/spending_processed.csv'
    FILEFORMAT = CSV
    FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true');
```

### Run dbt

```bash
pip install dbt-databricks

# Edit ~/.dbt/profiles.yml: set target to 'databricks'
dbt deps && dbt seed && dbt run && dbt test
```

### SQL Adjustments

| PostgreSQL | Databricks SQL Equivalent |
|---|---|
| `serial` | `BIGINT GENERATED ALWAYS AS IDENTITY` |
| `varchar(n)` | `STRING` |
| `boolean` | `BOOLEAN` (same) |
| `percentile_cont(0.75) within group (order by x)` | `PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY x)` (same) |
| `current_timestamp` | `current_timestamp()` |
| `(array_agg(x order by y))[1]` | `FIRST_VALUE(x) OVER (ORDER BY y DESC)` or `ARRAY_AGG(x ORDER BY y)[0]` |

### Delta Lake Features
- All tables are automatically Delta format (ACID transactions, time travel)
- Use `DESCRIBE HISTORY table_name` to view change history
- SCD Type 2 snapshot pattern works natively with Delta `MERGE`

---

## Cross-Platform Compatibility Summary

| Feature | PostgreSQL | Snowflake | BigQuery | Databricks |
|---|---|---|---|---|
| dbt adapter | dbt-postgres | dbt-snowflake | dbt-bigquery | dbt-databricks |
| `dbt_utils` support | Full | Full | Full | Full |
| `generate_surrogate_key` | md5 hash | md5 hash | md5 hash | md5 hash |
| Incremental strategy | `delete+insert` | `merge` | `merge` | `merge` |
| Snapshot strategy | `check` | `check` | `check` | `check` |
| Clustering | B-tree indexes | `CLUSTER BY` | Partition + cluster | Z-ORDER |
| Cost model | Fixed (local) | Per-second compute | Per-TB scanned | Per-DBU |
