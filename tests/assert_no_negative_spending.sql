-- tests/assert_no_negative_spending.sql
-- Validates that no drug has negative total spending.
-- Negative spending would indicate a data quality issue in the CMS source.

select
    generic_name,
    brand_name,
    total_spending
from {{ ref('stg_drug_spending') }}
where total_spending < 0
