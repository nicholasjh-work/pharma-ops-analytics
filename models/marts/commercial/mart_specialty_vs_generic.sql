-- mart_specialty_vs_generic.sql
-- KPI: Specialty drug spending concentration (Pareto analysis)
-- Grain: drug (generic_name + brand_name) + drug_year
-- BU: Commercial

with drug_data as (
    select * from {{ ref('int_drug_enriched') }}
    where total_beneficiaries >= {{ var('min_beneficiary_threshold') }}
      and total_spending > 0
),

ranked as (
    select
        drug_year,
        brand_name,
        generic_name,
        coalesce(therapeutic_area, 'Other') as therapeutic_area,
        is_specialty_drug,
        brand_generic_flag,
        total_claims,
        total_beneficiaries,
        total_spending,
        cost_per_claim,
        cost_per_beneficiary_monthly,

        -- Rank by spending for Pareto
        row_number() over (
            partition by drug_year
            order by total_spending desc
        ) as spending_rank,

        -- Running cumulative share
        round(
            sum(total_spending) over (
                partition by drug_year
                order by total_spending desc
                rows between unbounded preceding and current row
            ) / nullif(sum(total_spending) over (partition by drug_year), 0) * 100,
        2) as cumulative_spending_pct

    from drug_data
)

select
    {{ dbt_utils.generate_surrogate_key(['drug_year', 'generic_name', 'brand_name', 'manufacturer_name']) }}
        as specialty_key,
    *,
    case
        when cumulative_spending_pct <= 80 then 'Top 80%'
        when cumulative_spending_pct <= 95 then 'Next 15%'
        else 'Long Tail'
    end as pareto_tier,
    current_timestamp as _loaded_at
from ranked
