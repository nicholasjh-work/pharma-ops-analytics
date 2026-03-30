-- tests/assert_brand_generic_sum.sql
-- Validates that brand_claims + generic_claims approximately equals total_claims.
-- Allows 5% tolerance for rounding and suppressed records.

select
    npi,
    total_claims,
    brand_claims,
    generic_claims,
    (brand_claims + generic_claims) as computed_total,
    abs(total_claims - (brand_claims + generic_claims)) as diff
from {{ ref('stg_prescribers') }}
where total_claims > 0
  and brand_claims is not null
  and generic_claims is not null
  and abs(total_claims - (brand_claims + generic_claims))::numeric / total_claims > 0.05
