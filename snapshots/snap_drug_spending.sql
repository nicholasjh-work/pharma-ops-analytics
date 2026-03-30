-- snapshots/snap_drug_spending.sql
-- SCD Type 2 snapshot tracking drug pricing changes over time.
-- When new CMS annual data is loaded, this captures the change history
-- for avg_spending_per_claim and avg_spending_per_beneficiary.
--
-- In production with Snowflake, this enables "as-of" queries:
-- "What was the cost of Adalimumab in 2021 vs 2023?"

{% snapshot snap_drug_spending %}

{{
    config(
        target_schema='snapshots',
        unique_key='generic_name',
        strategy='check',
        check_cols=['avg_spending_per_claim', 'avg_spending_per_beneficiary', 'total_spending'],
    )
}}

select
    generic_name,
    brand_name,
    drug_year,
    total_claims,
    total_beneficiaries,
    total_spending,
    avg_spending_per_claim,
    avg_spending_per_beneficiary,
    current_timestamp as snapshot_loaded_at
from {{ ref('stg_drug_spending') }}

{% endsnapshot %}
