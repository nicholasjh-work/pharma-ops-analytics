-- mart_brand_generic_ratio.sql
-- KPI: Brand vs generic dispensing ratio by therapeutic area
-- Grain: therapeutic_area + state + drug_year
-- BU: Pharmacy Operations

with detail as (
    select * from {{ ref('int_prescriber_drug_detail') }}
    where total_claims > 0
      and therapeutic_area is not null
),

aggregated as (
    select
        prescriber_drug_year             as drug_year,
        therapeutic_area,
        is_gifthealth_focus,
        state,
        census_region,

        sum(total_claims)                as total_claims,
        sum(total_drug_cost)             as total_drug_cost,

        -- Brand identification: if brand_name is populated and differs from generic
        sum(case
            when brand_name is not null
                and brand_name != ''
                and brand_name != generic_name
            then total_claims else 0
        end) as brand_claims,

        sum(case
            when brand_name is null
                or brand_name = ''
                or brand_name = generic_name
            then total_claims else 0
        end) as generic_claims,

        sum(case
            when brand_name is not null
                and brand_name != ''
                and brand_name != generic_name
            then total_drug_cost else 0
        end) as brand_cost,

        sum(case
            when brand_name is null
                or brand_name = ''
                or brand_name = generic_name
            then total_drug_cost else 0
        end) as generic_cost,

        count(distinct npi)              as prescriber_count

    from detail
    group by 1, 2, 3, 4, 5
),

with_ratios as (
    select
        *,
        case
            when total_claims > 0
            then round(generic_claims::numeric / total_claims * 100, 1)
            else 0
        end as generic_dispensing_rate_pct,
        case
            when total_claims > 0
            then round(brand_claims::numeric / total_claims * 100, 1)
            else 0
        end as brand_dispensing_rate_pct,
        case
            when generic_cost > 0
            then round(brand_cost / generic_cost, 2)
            else null
        end as brand_to_generic_cost_ratio
    from aggregated
)

select
    {{ dbt_utils.generate_surrogate_key(['drug_year', 'therapeutic_area', 'state']) }}
        as brand_generic_key,
    *,
    current_timestamp as _loaded_at
from with_ratios
