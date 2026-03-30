-- mart_dq_completeness.sql
-- KPI: Data completeness (% non-null) per source table
-- Grain: source_table + column_name
-- BU: Data Quality / Governance

-- This model checks completeness of key columns across staging models.
-- In production, this would run on every refresh. For the portfolio
-- project, it demonstrates the governance pattern.

with spending_checks as (
    select
        'stg_drug_spending' as source_model,
        count(*) as total_rows,
        round(count(generic_name)::numeric / nullif(count(*), 0) * 100, 1)
            as generic_name_pct,
        round(count(brand_name) filter (where brand_name != '')::numeric
            / nullif(count(*), 0) * 100, 1)
            as brand_name_pct,
        round(count(total_spending)::numeric / nullif(count(*), 0) * 100, 1)
            as total_spending_pct,
        round(count(total_claims)::numeric / nullif(count(*), 0) * 100, 1)
            as total_claims_pct,
        round(count(total_beneficiaries)::numeric / nullif(count(*), 0) * 100, 1)
            as total_beneficiaries_pct
    from {{ ref('stg_drug_spending') }}
),

prescriber_checks as (
    select
        'stg_prescribers' as source_model,
        count(*) as total_rows,
        round(count(npi)::numeric / nullif(count(*), 0) * 100, 1)
            as npi_pct,
        round(count(specialty)::numeric / nullif(count(*), 0) * 100, 1)
            as specialty_pct,
        round(count(state)::numeric / nullif(count(*), 0) * 100, 1)
            as state_pct,
        round(count(total_claims)::numeric / nullif(count(*), 0) * 100, 1)
            as total_claims_pct,
        round(count(total_drug_cost)::numeric / nullif(count(*), 0) * 100, 1)
            as total_drug_cost_pct
    from {{ ref('stg_prescribers') }}
),

-- Unpivot into a standard format
results as (
    select source_model, total_rows, 'generic_name' as column_name, generic_name_pct as completeness_pct from spending_checks
    union all
    select source_model, total_rows, 'brand_name', brand_name_pct from spending_checks
    union all
    select source_model, total_rows, 'total_spending', total_spending_pct from spending_checks
    union all
    select source_model, total_rows, 'total_claims', total_claims_pct from spending_checks
    union all
    select source_model, total_rows, 'total_beneficiaries', total_beneficiaries_pct from spending_checks
    union all
    select source_model, total_rows, 'npi', npi_pct from prescriber_checks
    union all
    select source_model, total_rows, 'specialty', specialty_pct from prescriber_checks
    union all
    select source_model, total_rows, 'state', state_pct from prescriber_checks
    union all
    select source_model, total_rows, 'total_claims', total_claims_pct from prescriber_checks
    union all
    select source_model, total_rows, 'total_drug_cost', total_drug_cost_pct from prescriber_checks
)

select
    source_model,
    total_rows,
    column_name,
    completeness_pct,
    case
        when completeness_pct >= 99 then 'Pass'
        when completeness_pct >= 95 then 'Warning'
        else 'Fail'
    end as status,
    current_timestamp as checked_at
from results
