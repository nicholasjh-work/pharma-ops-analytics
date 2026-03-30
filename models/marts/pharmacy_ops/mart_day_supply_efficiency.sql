-- mart_day_supply_efficiency.sql
-- KPI: Average day supply per claim, cost per day of therapy
-- Grain: generic_name + therapeutic_area + state + drug_year
-- BU: Pharmacy Operations

with detail as (
    select * from {{ ref('int_prescriber_drug_detail') }}
    where total_claims > 0
      and total_day_supply > 0
      and therapeutic_area is not null
),

aggregated as (
    select
        prescriber_drug_year                     as drug_year,
        generic_name,
        therapeutic_area,
        is_specialty_drug,
        is_gifthealth_focus,
        state,
        census_region,

        count(distinct npi)                      as prescriber_count,
        sum(total_claims)                        as total_claims,
        sum(total_day_supply)                    as total_day_supply,
        sum(total_drug_cost)                     as total_drug_cost,
        sum(total_beneficiaries)                 as total_beneficiaries,

        -- Day supply efficiency
        round(sum(total_day_supply)::numeric / nullif(sum(total_claims), 0), 1)
            as avg_day_supply_per_claim,

        -- Cost per day of therapy
        round(sum(total_drug_cost) / nullif(sum(total_day_supply), 0), 2)
            as cost_per_day_of_therapy,

        -- 30-day equivalent cost
        round(sum(total_drug_cost) / nullif(sum(total_day_supply), 0) * 30, 2)
            as cost_per_30day_equivalent

    from detail
    group by 1, 2, 3, 4, 5, 6, 7
)

select
    {{ dbt_utils.generate_surrogate_key([
        'drug_year', 'generic_name', 'state'
    ]) }} as day_supply_key,
    *,
    current_timestamp as _loaded_at
from aggregated
