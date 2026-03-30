-- mart_prescriber_retention_cohort.sql
-- KPI: Prescriber volume cohorts by specialty and engagement tier
-- Grain: specialty + engagement_tier + census_region + prescriber_year
-- BU: Customer Success
-- Note: True retention requires multi-year data. This model builds
-- the cohort structure that supports retention analysis when
-- multiple years are loaded.

with prescribers as (
    select * from {{ ref('int_prescriber_summary') }}
    where total_claims > 0
),

cohorts as (
    select
        prescriber_year,
        specialty,
        census_region,
        engagement_tier,

        count(distinct npi)                           as prescriber_count,
        sum(total_claims)                             as total_claims,
        sum(total_drug_cost)                          as total_drug_cost,
        round(avg(total_claims), 0)                   as avg_claims_per_prescriber,
        round(avg(total_drug_cost), 2)                as avg_cost_per_prescriber,
        round(avg(brand_prescribing_rate_pct), 1)     as avg_brand_rate_pct,
        round(avg(avg_beneficiary_age), 1)            as avg_patient_age,

        -- Distribution within cohort
        percentile_cont(0.50) within group (order by total_claims)
            as median_claims,
        percentile_cont(0.90) within group (order by total_claims)
            as p90_claims

    from prescribers
    group by 1, 2, 3, 4
)

select
    {{ dbt_utils.generate_surrogate_key([
        'prescriber_year', 'specialty', 'census_region', 'engagement_tier'
    ]) }} as cohort_key,
    *,
    current_timestamp as _loaded_at
from cohorts
