-- stg_drug_spending.sql
-- Staging model for CMS Medicare Part D Spending by Drug
-- Grain: one row per drug (brand/generic) per year
-- Source: data.cms.gov DSD_PTD

with source as (
    select * from {{ source('raw', 'raw_part_d_spending') }}
),

renamed as (
    select
        -- Drug identification
        trim(upper(coalesce(brnd_name, '')))    as brand_name,
        trim(upper(gnrc_name))                   as generic_name,

        -- Utilization
        cast(tot_clms as bigint)                 as total_claims,
        cast(tot_benes as bigint)                as total_beneficiaries,
        cast(tot_30day_fills as bigint)          as total_30day_fills,

        -- Spending
        cast(tot_spndng as numeric(18,2))        as total_spending,
        cast(avg_spnd_per_clm as numeric(12,2))  as avg_spending_per_claim,
        cast(avg_spnd_per_bene as numeric(12,2)) as avg_spending_per_beneficiary,

        -- Year-over-year
        cast(chg_in_avg_spnd_per_clm as numeric(12,2))  as yoy_change_avg_cost_per_claim,
        cast(avg_spnd_per_clm_ly as numeric(12,2))       as avg_cost_per_claim_prior_year,

        -- Outlier flag
        case
            when outlier_flag is not null
                and trim(outlier_flag) != ''
            then true
            else false
        end as is_outlier,

        2023 as drug_year

    from source
    where gnrc_name is not null
      and trim(gnrc_name) != ''
)

select * from renamed
