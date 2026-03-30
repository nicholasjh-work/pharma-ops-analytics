-- mart_therapeutic_area_mix.sql
-- KPI: Market size and growth by therapeutic area
-- Grain: therapeutic_area + brand_generic_flag + drug_year
-- BU: Commercial

with drug_data as (
    select * from {{ ref('int_drug_enriched') }}
    where total_beneficiaries >= {{ var('min_beneficiary_threshold') }}
),

by_area as (
    select
        drug_year,
        coalesce(therapeutic_area, 'Other')  as therapeutic_area,
        brand_generic_flag,

        count(distinct generic_name)         as drug_count,
        sum(total_claims)                    as total_claims,
        sum(total_spending)                  as total_spending,
        sum(total_beneficiaries)             as total_beneficiaries,
        round(avg(cost_per_claim), 2)        as avg_cost_per_claim,
        round(avg(cost_per_beneficiary_monthly), 2) as avg_monthly_cost

    from drug_data
    group by 1, 2, 3
),

with_share as (
    select
        *,
        round(
            total_spending / nullif(sum(total_spending) over (partition by drug_year), 0) * 100,
        2) as spending_share_pct,
        round(
            total_claims::numeric / nullif(sum(total_claims) over (partition by drug_year), 0) * 100,
        2) as claims_share_pct
    from by_area
)

select
    {{ dbt_utils.generate_surrogate_key(['drug_year', 'therapeutic_area', 'brand_generic_flag']) }}
        as ta_mix_key,
    *,
    current_timestamp as _loaded_at
from with_share
