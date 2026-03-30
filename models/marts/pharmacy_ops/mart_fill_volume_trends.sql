-- mart_fill_volume_trends.sql
-- KPI: Prescription fill volume by therapeutic area and drug type
-- Grain: therapeutic_area + is_specialty_drug + drug_year
-- BU: Pharmacy Operations

with drug_data as (
    select * from {{ ref('int_drug_enriched') }}
    where total_beneficiaries >= {{ var('min_beneficiary_threshold') }}
),

fill_trends as (
    select
        drug_year,
        coalesce(therapeutic_area, 'Other') as therapeutic_area,
        is_gifthealth_focus,
        is_specialty_drug,
        brand_generic_flag,

        count(distinct generic_name)         as distinct_drugs,
        sum(total_claims)                    as total_claims,
        sum(total_30day_fills)               as total_30day_fills,
        sum(total_beneficiaries)             as total_beneficiaries,
        sum(total_spending)                  as total_spending,

        round(avg(cost_per_claim), 2)        as avg_cost_per_claim,
        round(avg(cost_per_beneficiary_monthly), 2) as avg_monthly_cost_per_bene,

        -- Efficiency: 30-day fills per beneficiary (proxy for adherence)
        case
            when sum(total_beneficiaries) > 0
            then round(sum(total_30day_fills)::numeric / sum(total_beneficiaries), 2)
            else null
        end as fills_per_beneficiary

    from drug_data
    group by 1, 2, 3, 4, 5
)

select
    {{ dbt_utils.generate_surrogate_key([
        'drug_year', 'therapeutic_area', 'is_gifthealth_focus', 'is_specialty_drug', 'brand_generic_flag'
    ]) }} as fill_trend_key,
    *,
    current_timestamp as _loaded_at
from fill_trends
