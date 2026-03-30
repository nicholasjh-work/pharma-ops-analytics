-- mart_prescriber_engagement.sql
-- KPI: Prescriber engagement tier, brand rate, volume by specialty
-- Grain: prescriber (NPI) + prescriber_year
-- BU: Customer Success

with prescribers as (
    select * from {{ ref('int_prescriber_summary') }}
    where total_claims > 0
),

engagement as (
    select
        prescriber_key,
        prescriber_year,
        npi,
        last_name_or_org,
        first_name,
        credentials,
        specialty,
        state,
        city,
        census_region,

        engagement_tier,
        total_claims,
        total_beneficiaries,
        total_drug_cost,
        brand_claims,
        generic_claims,
        brand_prescribing_rate_pct,
        avg_cost_per_claim,
        claims_per_beneficiary,

        opioid_claims,
        antibiotic_claims,
        avg_beneficiary_age,

        -- High-value prescriber flag (top quartile claims + high brand rate)
        case
            when engagement_tier = 'High'
                and brand_prescribing_rate_pct > 50
            then true
            else false
        end as is_high_value_brand_prescriber,

        -- Opioid risk flag
        case
            when opioid_claims > 0
                and total_claims > 0
                and (opioid_claims::numeric / total_claims) > 0.10
            then true
            else false
        end as opioid_high_rate_flag

    from prescribers
)

select
    *,
    current_timestamp as _loaded_at
from engagement
