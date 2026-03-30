-- mart_drug_spending_trends.sql
-- KPI: Total spending, YoY change, claims volume by therapeutic area
-- Grain: therapeutic_area + drug_year
-- Owner: Commercial analytics
-- BU: Commercial

with drug_data as (
    select * from {{ ref('int_drug_enriched') }}
    where total_beneficiaries >= {{ var('min_beneficiary_threshold') }}
),

aggregated as (
    select
        drug_year,
        coalesce(therapeutic_area, 'Other') as therapeutic_area,
        is_gifthealth_focus,

        count(distinct generic_name)                    as distinct_drugs,
        sum(total_claims)                               as total_claims,
        sum(total_beneficiaries)                        as total_beneficiaries,
        sum(total_spending)                             as total_spending,
        round(avg(cost_per_claim), 2)                   as avg_cost_per_claim,
        round(avg(cost_per_beneficiary_monthly), 2)     as avg_monthly_cost_per_bene,

        sum(case when is_specialty_drug then total_spending else 0 end)
            as specialty_spending,
        sum(case when not is_specialty_drug then total_spending else 0 end)
            as non_specialty_spending,

        sum(case when brand_generic_flag = 'Brand' then total_spending else 0 end)
            as brand_spending,
        sum(case when brand_generic_flag = 'Generic' then total_spending else 0 end)
            as generic_spending

    from drug_data
    group by 1, 2, 3
)

select
    {{ dbt_utils.generate_surrogate_key(['drug_year', 'therapeutic_area', 'is_gifthealth_focus']) }}
        as spending_trend_key,
    *,
    case
        when total_spending > 0
        then round(specialty_spending / total_spending * 100, 1)
        else 0
    end as specialty_pct,
    case
        when total_spending > 0
        then round(brand_spending / total_spending * 100, 1)
        else 0
    end as brand_pct,
    current_timestamp as _loaded_at
from aggregated
