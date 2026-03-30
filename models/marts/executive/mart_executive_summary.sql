-- mart_executive_summary.sql
-- KPI: Executive summary combining drug spending and prescriber metrics
-- Grain: therapeutic_area + drug_year
-- BU: Executive

with spending_by_ta as (
    select
        drug_year,
        coalesce(therapeutic_area, 'Other') as therapeutic_area,
        is_gifthealth_focus,
        sum(total_spending)                 as total_spending,
        sum(total_claims)                   as total_claims,
        sum(total_beneficiaries)            as total_beneficiaries,
        count(distinct generic_name)        as drug_count,
        round(avg(cost_per_claim), 2)       as avg_cost_per_claim,
        round(avg(cost_per_beneficiary_monthly), 2) as avg_monthly_cost
    from {{ ref('int_drug_enriched') }}
    where total_beneficiaries >= {{ var('min_beneficiary_threshold') }}
    group by 1, 2, 3
),

prescribers_by_ta as (
    select
        prescriber_drug_year                as drug_year,
        coalesce(therapeutic_area, 'Other') as therapeutic_area,
        count(distinct npi)                 as prescriber_count,
        sum(total_claims)                   as prescriber_claims,
        sum(total_drug_cost)                as prescriber_drug_cost,
        count(distinct state)               as state_reach
    from {{ ref('int_prescriber_drug_detail') }}
    where total_claims > 0
      and therapeutic_area is not null
    group by 1, 2
),

combined as (
    select
        s.drug_year,
        s.therapeutic_area,
        s.is_gifthealth_focus,

        -- Drug-level metrics
        s.drug_count,
        s.total_spending,
        s.total_claims,
        s.total_beneficiaries,
        s.avg_cost_per_claim,
        s.avg_monthly_cost,

        -- Prescriber metrics
        coalesce(p.prescriber_count, 0)      as prescriber_count,
        coalesce(p.state_reach, 0)           as state_reach,

        -- Spending share
        round(
            s.total_spending / nullif(sum(s.total_spending) over (partition by s.drug_year), 0) * 100,
        2) as spending_share_pct

    from spending_by_ta s
    left join prescribers_by_ta p
        on s.drug_year = p.drug_year
        and s.therapeutic_area = p.therapeutic_area
)

select
    {{ dbt_utils.generate_surrogate_key(['drug_year', 'therapeutic_area', 'is_gifthealth_focus']) }}
        as exec_summary_key,
    *,
    current_timestamp as _loaded_at
from combined
