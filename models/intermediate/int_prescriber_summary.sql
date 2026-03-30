-- int_prescriber_summary.sql
-- Enriches prescriber data with census region and engagement tier.
-- Grain: one row per prescriber (NPI) per year

with prescribers as (
    select * from {{ ref('stg_prescribers') }}
),

regions as (
    select
        state_abbrev,
        census_region,
        census_division
    from {{ ref('seed_state_region_map') }}
),

with_region as (
    select
        p.*,
        r.census_region,
        r.census_division,

        -- Brand prescribing rate
        case
            when (p.brand_claims + p.generic_claims) > 0
            then round(
                p.brand_claims::numeric / (p.brand_claims + p.generic_claims) * 100, 1
            )
            else null
        end as brand_prescribing_rate_pct,

        -- Average cost per claim
        case
            when p.total_claims > 0
            then round(p.total_drug_cost / p.total_claims, 2)
            else null
        end as avg_cost_per_claim,

        -- Average claims per beneficiary
        case
            when p.total_beneficiaries > 0
            then round(p.total_claims::numeric / p.total_beneficiaries, 1)
            else null
        end as claims_per_beneficiary

    from prescribers p
    left join regions r
        on p.state = r.state_abbrev
),

-- Calculate engagement tier using percentiles across all prescribers
percentiles as (
    select
        percentile_cont(0.75) within group (order by total_claims) as p75_claims,
        percentile_cont(0.25) within group (order by total_claims) as p25_claims
    from with_region
    where total_claims > 0
),

tiered as (
    select
        w.*,
        case
            when w.total_claims >= p.p75_claims then 'High'
            when w.total_claims >= p.p25_claims then 'Medium'
            else 'Low'
        end as engagement_tier
    from with_region w
    cross join percentiles p
)

select
    {{ dbt_utils.generate_surrogate_key(['prescriber_year', 'npi']) }}
        as prescriber_key,
    *
from tiered
