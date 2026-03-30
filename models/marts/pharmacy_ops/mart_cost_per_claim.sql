-- mart_cost_per_claim.sql
-- KPI: Cost Per Claim = total_drug_cost / total_claims
-- Grain: drug (generic_name) + therapeutic_area + drug_year
-- Owner: Data & Analytics
-- Exclusions: suppressed cells (<11 beneficiaries)
-- BU: Pharmacy Operations

with drug_data as (
    select * from {{ ref('int_drug_enriched') }}
    where total_beneficiaries >= {{ var('min_beneficiary_threshold') }}
      and total_claims > 0
),

cost_metrics as (
    select
        drug_year,
        brand_name,
        generic_name,
        coalesce(therapeutic_area, 'Other') as therapeutic_area,
        is_gifthealth_focus,
        is_specialty_drug,
        brand_generic_flag,

        total_claims,
        total_beneficiaries,
        total_spending,
        total_30day_fills,

        cost_per_claim,
        cost_per_beneficiary,
        cost_per_beneficiary_monthly,

        -- Cost efficiency: spending per 30-day fill
        case
            when total_30day_fills > 0
            then round(total_spending / total_30day_fills, 2)
            else null
        end as cost_per_30day_fill,

        -- Exceeds CMS specialty threshold
        case
            when cost_per_beneficiary_monthly > {{ var('specialty_cost_threshold') }}
            then true
            else false
        end as exceeds_specialty_threshold

    from drug_data
)

select
    {{ dbt_utils.generate_surrogate_key(['drug_year', 'generic_name', 'brand_name']) }}
        as cost_claim_key,
    *,
    current_timestamp as _loaded_at
from cost_metrics
