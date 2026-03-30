-- mart_dq_anomalies.sql
-- KPI: Statistical outliers in drug spending (>3 sigma from mean)
-- Grain: drug (generic_name) + drug_year
-- BU: Data Quality / Governance

with drug_data as (
    select
        drug_year,
        generic_name,
        brand_name,
        coalesce(therapeutic_area, 'Other') as therapeutic_area,
        total_spending,
        cost_per_claim,
        cost_per_beneficiary_monthly,
        total_claims,
        total_beneficiaries
    from {{ ref('int_drug_enriched') }}
    where total_beneficiaries >= {{ var('min_beneficiary_threshold') }}
      and total_claims > 0
),

-- Calculate mean and stddev per therapeutic area
stats as (
    select
        drug_year,
        therapeutic_area,
        avg(cost_per_claim)    as mean_cost_per_claim,
        stddev(cost_per_claim) as stddev_cost_per_claim,
        avg(cost_per_beneficiary_monthly)    as mean_monthly_cost,
        stddev(cost_per_beneficiary_monthly) as stddev_monthly_cost
    from drug_data
    where cost_per_claim is not null
    group by 1, 2
    having count(*) >= 5  -- need enough data points for meaningful stats
),

with_zscore as (
    select
        d.*,
        s.mean_cost_per_claim,
        s.stddev_cost_per_claim,
        s.mean_monthly_cost,
        s.stddev_monthly_cost,

        case
            when s.stddev_cost_per_claim > 0
            then round((d.cost_per_claim - s.mean_cost_per_claim) / s.stddev_cost_per_claim, 2)
            else 0
        end as cost_per_claim_zscore,

        case
            when s.stddev_monthly_cost > 0
            then round(
                (d.cost_per_beneficiary_monthly - s.mean_monthly_cost) / s.stddev_monthly_cost, 2
            )
            else 0
        end as monthly_cost_zscore

    from drug_data d
    left join stats s
        on d.drug_year = s.drug_year
        and d.therapeutic_area = s.therapeutic_area
)

select
    drug_year,
    generic_name,
    brand_name,
    therapeutic_area,
    total_spending,
    cost_per_claim,
    cost_per_beneficiary_monthly,
    total_claims,
    total_beneficiaries,
    cost_per_claim_zscore,
    monthly_cost_zscore,
    case
        when abs(cost_per_claim_zscore) > 3
            or abs(monthly_cost_zscore) > 3
        then 'Anomaly'
        when abs(cost_per_claim_zscore) > 2
            or abs(monthly_cost_zscore) > 2
        then 'Warning'
        else 'Normal'
    end as anomaly_status,
    current_timestamp as checked_at
from with_zscore
where abs(cost_per_claim_zscore) > 2
   or abs(monthly_cost_zscore) > 2
order by abs(cost_per_claim_zscore) desc
