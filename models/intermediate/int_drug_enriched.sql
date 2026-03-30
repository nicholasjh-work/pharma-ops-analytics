-- int_drug_enriched.sql
-- Enriches drug spending data with therapeutic area classification
-- and specialty drug flags from the seed mapping table.
-- Grain: one row per drug per year

with spending as (
    select * from {{ ref('stg_drug_spending') }}
),

therapeutic_map as (
    select
        upper(trim(generic_name))  as generic_name,
        therapeutic_area,
        is_specialty::boolean      as is_specialty_mapped,
        is_gifthealth_focus::boolean as is_gifthealth_focus
    from {{ ref('seed_therapeutic_area_map') }}
),

enriched as (
    select
        s.*,

        -- Therapeutic area from seed map (null if not in map)
        t.therapeutic_area,
        coalesce(t.is_gifthealth_focus, false) as is_gifthealth_focus,

        -- Specialty drug: use mapped value if available,
        -- otherwise derive from CMS $830/month threshold
        coalesce(
            t.is_specialty_mapped,
            case
                when s.total_beneficiaries > 0
                    and (s.total_spending / s.total_beneficiaries) / 12
                        > {{ var('specialty_cost_threshold') }}
                then true
                else false
            end
        ) as is_specialty_drug,

        -- Derived metrics
        case
            when s.total_claims > 0
            then round(s.total_spending / s.total_claims, 2)
            else null
        end as cost_per_claim,

        case
            when s.total_beneficiaries > 0
            then round(s.total_spending / s.total_beneficiaries, 2)
            else null
        end as cost_per_beneficiary,

        case
            when s.total_beneficiaries > 0
            then round((s.total_spending / s.total_beneficiaries) / 12, 2)
            else null
        end as cost_per_beneficiary_monthly,

        -- Classification
        case
            when s.brand_name is not null and s.brand_name != ''
            then 'Brand'
            else 'Generic'
        end as brand_generic_flag

    from spending s
    left join therapeutic_map t
        on s.generic_name = t.generic_name
)

select
    {{ dbt_utils.generate_surrogate_key(['drug_year', 'generic_name', 'brand_name']) }}
        as drug_spending_key,
    *
from enriched
