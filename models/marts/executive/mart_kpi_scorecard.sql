-- mart_kpi_scorecard.sql
-- KPI: Top-level executive scorecard with cross-BU metrics
-- Grain: drug_year (one row per year)
-- BU: Executive

with spending as (
    select * from {{ ref('int_drug_enriched') }}
    where total_beneficiaries >= {{ var('min_beneficiary_threshold') }}
),

prescribers as (
    select * from {{ ref('int_prescriber_summary') }}
    where total_claims > 0
),

scorecard as (
    select
        s.drug_year,

        -- Volume KPIs
        count(distinct s.generic_name)                   as total_drugs,
        sum(s.total_claims)                              as total_claims,
        sum(s.total_beneficiaries)                       as total_beneficiaries,
        sum(s.total_spending)                            as total_spending,

        -- Cost KPIs
        round(avg(s.cost_per_claim), 2)                  as avg_cost_per_claim,
        round(avg(s.cost_per_beneficiary_monthly), 2)    as avg_monthly_cost_per_bene,

        -- Mix KPIs
        round(
            sum(case when s.is_specialty_drug then s.total_spending else 0 end)
            / nullif(sum(s.total_spending), 0) * 100, 1
        ) as specialty_spending_pct,

        round(
            sum(case when s.brand_generic_flag = 'Brand' then s.total_spending else 0 end)
            / nullif(sum(s.total_spending), 0) * 100, 1
        ) as brand_spending_pct,

        -- Gifthealth focus area KPIs
        sum(case when s.is_gifthealth_focus then s.total_spending else 0 end)
            as gifthealth_focus_spending,
        sum(case when s.is_gifthealth_focus then s.total_claims else 0 end)
            as gifthealth_focus_claims,

        -- Therapeutic area count
        count(distinct s.therapeutic_area)
            filter (where s.therapeutic_area is not null) as therapeutic_area_count

    from spending s
    group by 1
)

select
    *,
    current_timestamp as _loaded_at
from scorecard
