-- int_prescriber_drug_detail.sql
-- Joins prescriber-drug data with therapeutic area classification.
-- Grain: one row per prescriber per drug per year

with prescriber_drugs as (
    select * from {{ ref('stg_prescriber_drugs') }}
),

therapeutic_map as (
    select
        upper(trim(generic_name))    as generic_name,
        therapeutic_area,
        is_specialty::boolean        as is_specialty,
        is_gifthealth_focus::boolean as is_gifthealth_focus
    from {{ ref('seed_therapeutic_area_map') }}
),

regions as (
    select state_abbrev, census_region
    from {{ ref('seed_state_region_map') }}
),

enriched as (
    select
        pd.*,
        t.therapeutic_area,
        coalesce(t.is_specialty, false) as is_specialty_drug,
        coalesce(t.is_gifthealth_focus, false) as is_gifthealth_focus,
        r.census_region,

        -- Derived: cost per claim at prescriber-drug level
        case
            when pd.total_claims > 0
            then round(pd.total_drug_cost / pd.total_claims, 2)
            else null
        end as cost_per_claim,

        -- Derived: average day supply per claim
        case
            when pd.total_claims > 0
            then round(pd.total_day_supply::numeric / pd.total_claims, 1)
            else null
        end as avg_day_supply_per_claim

    from prescriber_drugs pd
    left join therapeutic_map t
        on pd.generic_name = t.generic_name
    left join regions r
        on pd.state = r.state_abbrev
)

select
    {{ dbt_utils.generate_surrogate_key([
        'prescriber_drug_year', 'npi', 'generic_name', 'brand_name'
    ]) }} as prescriber_drug_key,
    *
from enriched
