-- mart_geographic_distribution.sql
-- KPI: Prescribing volume and cost by state and therapeutic area
-- Grain: state + therapeutic_area + drug_year
-- BU: Enterprise Customers

with detail as (
    select * from {{ ref('int_prescriber_drug_detail') }}
    where total_claims > 0
      and state is not null
),

geo as (
    select
        prescriber_drug_year                  as drug_year,
        state,
        census_region,
        coalesce(therapeutic_area, 'Other')   as therapeutic_area,
        is_gifthealth_focus,

        count(distinct npi)                   as prescriber_count,
        count(distinct generic_name)          as distinct_drugs,
        sum(total_claims)                     as total_claims,
        sum(total_drug_cost)                  as total_drug_cost,
        sum(total_beneficiaries)              as total_beneficiaries,
        sum(total_day_supply)                 as total_day_supply,

        round(sum(total_drug_cost) / nullif(sum(total_claims), 0), 2)
            as avg_cost_per_claim,
        round(sum(total_drug_cost) / nullif(sum(total_beneficiaries), 0), 2)
            as cost_per_beneficiary,
        round(sum(total_claims)::numeric / nullif(count(distinct npi), 0), 0)
            as claims_per_prescriber

    from detail
    group by 1, 2, 3, 4, 5
)

select
    {{ dbt_utils.generate_surrogate_key(['drug_year', 'state', 'therapeutic_area', 'is_gifthealth_focus']) }}
        as geo_key,
    *,
    current_timestamp as _loaded_at
from geo
