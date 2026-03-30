-- mart_specialty_adoption_funnel.sql
-- KPI: Specialty drug adoption by prescriber specialty and therapeutic area
-- Grain: prescriber_specialty + therapeutic_area + drug_year
-- BU: Customer Success

with prescriber_drugs as (
    select * from {{ ref('int_prescriber_drug_detail') }}
    where total_claims > 0
      and therapeutic_area is not null
),

funnel as (
    select
        prescriber_drug_year                         as drug_year,
        specialty                                    as prescriber_specialty,
        therapeutic_area,
        is_gifthealth_focus,

        -- Prescriber reach
        count(distinct npi)                          as prescriber_count,

        -- Volume
        sum(total_claims)                            as total_claims,
        sum(total_drug_cost)                         as total_drug_cost,
        sum(total_beneficiaries)                     as total_beneficiaries,
        sum(total_day_supply)                        as total_day_supply,

        -- Drug breadth
        count(distinct generic_name)                 as distinct_drugs_prescribed,

        -- Specialty drug adoption
        count(distinct case when is_specialty_drug then npi end)
            as prescribers_with_specialty_rx,
        sum(case when is_specialty_drug then total_claims else 0 end)
            as specialty_claims,
        sum(case when is_specialty_drug then total_drug_cost else 0 end)
            as specialty_drug_cost

    from prescriber_drugs
    group by 1, 2, 3, 4
),

with_rates as (
    select
        *,
        case
            when prescriber_count > 0
            then round(prescribers_with_specialty_rx::numeric / prescriber_count * 100, 1)
            else 0
        end as specialty_adoption_rate_pct,
        case
            when total_claims > 0
            then round(specialty_claims::numeric / total_claims * 100, 1)
            else 0
        end as specialty_claims_pct,
        case
            when total_claims > 0
            then round(total_drug_cost / total_claims, 2)
            else null
        end as avg_cost_per_claim
    from funnel
)

select
    {{ dbt_utils.generate_surrogate_key(['drug_year', 'prescriber_specialty', 'therapeutic_area', 'is_gifthealth_focus']) }}
        as adoption_key,
    *,
    current_timestamp as _loaded_at
from with_rates
