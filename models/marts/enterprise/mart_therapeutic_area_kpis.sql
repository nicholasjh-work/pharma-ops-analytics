-- mart_therapeutic_area_kpis.sql
-- KPI: Summary KPIs per therapeutic area for enterprise partner reporting
-- Grain: therapeutic_area + drug_year
-- BU: Enterprise Customers

with drug_data as (
    select * from {{ ref('int_drug_enriched') }}
    where total_beneficiaries >= {{ var('min_beneficiary_threshold') }}
),

kpis as (
    select
        drug_year,
        coalesce(therapeutic_area, 'Other')  as therapeutic_area,
        is_gifthealth_focus,

        -- Volume
        count(distinct generic_name)         as drug_count,
        count(distinct brand_name)
            filter (where brand_name != '')   as brand_count,
        sum(total_claims)                    as total_claims,
        sum(total_beneficiaries)             as total_beneficiaries,
        sum(total_30day_fills)               as total_30day_fills,

        -- Spending
        sum(total_spending)                  as total_spending,
        round(avg(cost_per_claim), 2)        as avg_cost_per_claim,
        round(avg(cost_per_beneficiary_monthly), 2) as avg_monthly_cost,

        -- Mix
        round(
            sum(case when is_specialty_drug then total_spending else 0 end)
            / nullif(sum(total_spending), 0) * 100, 1
        ) as specialty_spending_pct,

        round(
            sum(case when brand_generic_flag = 'Brand' then total_spending else 0 end)
            / nullif(sum(total_spending), 0) * 100, 1
        ) as brand_spending_pct,

        -- Top drug in area
        (array_agg(generic_name order by total_spending desc))[1]
            as top_drug_by_spending

    from drug_data
    group by 1, 2, 3
)

select
    {{ dbt_utils.generate_surrogate_key(['drug_year', 'therapeutic_area', 'is_gifthealth_focus']) }}
        as ta_kpi_key,
    *,
    current_timestamp as _loaded_at
from kpis
