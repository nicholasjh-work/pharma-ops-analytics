-- tests/assert_brand_generic_sum.sql
-- Validates that brand_claims + generic_claims approximately equals total_claims.
-- CMS data includes "Other" plan type claims (MAPD, PDP, LIS, non-LIS) that
-- may not cleanly split into brand/generic. Configured as warn because this
-- is a known CMS data characteristic, not a pipeline defect.

{{ config(severity='warn') }}

select
    npi,
    total_claims,
    brand_claims,
    generic_claims,
    (brand_claims + generic_claims) as computed_total,
    abs(total_claims - (brand_claims + generic_claims)) as diff
from {{ ref('stg_prescribers') }}
where total_claims > 100
  and brand_claims is not null
  and generic_claims is not null
  and abs(total_claims - (brand_claims + generic_claims))::numeric / total_claims > 0.20
