-- mart_manufacturer_performance_report.sql
-- KPI: Per-manufacturer drug performance (claims, spending, market share)
-- Grain: generic_name + brand_name + drug_year
-- BU: Enterprise Customers
-- Note: Simulates the partner reporting Gifthealth delivers to
-- pharmaceutical manufacturers. Each manufacturer sees only their drugs.

with drug_data as (
    select * from {{ ref('int_drug_enriched') }}
    where total_beneficiaries >= {{ var('min_beneficiary_threshold') }}
      and total_claims > 0
),

-- Total spending per therapeutic area for market share calculation
ta_totals as (
    select
        drug_year,
        coalesce(therapeutic_area, 'Other') as therapeutic_area,
        sum(total_spending) as ta_total_spending,
        sum(total_claims)   as ta_total_claims
    from drug_data
    group by 1, 2
),

manufacturer_report as (
    select
        d.drug_year,
        d.brand_name,
        d.generic_name,
        d.manufacturer_name,
        coalesce(d.therapeutic_area, 'Other') as therapeutic_area,
        d.is_gifthealth_focus,
        d.is_specialty_drug,
        d.brand_generic_flag,

        d.total_claims,
        d.total_beneficiaries,
        d.total_spending,
        d.total_30day_fills,
        d.cost_per_claim,
        d.cost_per_beneficiary,
        d.cost_per_beneficiary_monthly,

        -- Market share within therapeutic area
        round(
            d.total_spending / nullif(t.ta_total_spending, 0) * 100, 2
        ) as ta_spending_share_pct,
        round(
            d.total_claims::numeric / nullif(t.ta_total_claims, 0) * 100, 2
        ) as ta_claims_share_pct,

        t.ta_total_spending,
        t.ta_total_claims

    from drug_data d
    left join ta_totals t
        on d.drug_year = t.drug_year
        and coalesce(d.therapeutic_area, 'Other') = t.therapeutic_area
)

select
    {{ dbt_utils.generate_surrogate_key(['drug_year', 'generic_name', 'brand_name', 'manufacturer_name']) }}
        as mfr_report_key,
    *,
    current_timestamp as _loaded_at
from manufacturer_report
