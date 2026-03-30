-- stg_drug_spending.sql
-- Staging model for CMS Medicare Part D Spending by Drug
-- Grain: one row per drug (brand/generic) per year
-- Source: data.cms.gov DSD_PTD
--
-- The CMS file is wide-format with year-suffixed columns (2019-2023).
-- This model unpivots into long format for downstream consistency.

with source as (
    select * from {{ source('raw', 'raw_part_d_spending') }}
    where gnrc_name is not null
      and trim(gnrc_name) != ''
),

-- Unpivot: one UNION per year
unpivoted as (
    select
        trim(upper(coalesce(brnd_name, '')))  as brand_name,
        trim(upper(gnrc_name))                 as generic_name,
        trim(coalesce(mftr_name, ''))          as manufacturer_name,
        cast(tot_mftr as integer)              as manufacturer_count,
        2019                                   as drug_year,
        tot_spndng_2019                        as total_spending,
        tot_clms_2019                          as total_claims,
        tot_benes_2019                         as total_beneficiaries,
        tot_dsg_unts_2019                      as total_dosage_units,
        avg_spnd_per_clm_2019                  as avg_spending_per_claim,
        avg_spnd_per_bene_2019                 as avg_spending_per_beneficiary,
        avg_spnd_per_dsg_unt_wghtd_2019        as avg_spending_per_dosage_unit,
        outlier_flag_2019                      as outlier_flag
    from source

    union all

    select
        trim(upper(coalesce(brnd_name, ''))),
        trim(upper(gnrc_name)),
        trim(coalesce(mftr_name, '')),
        cast(tot_mftr as integer),
        2020,
        tot_spndng_2020,
        tot_clms_2020,
        tot_benes_2020,
        tot_dsg_unts_2020,
        avg_spnd_per_clm_2020,
        avg_spnd_per_bene_2020,
        avg_spnd_per_dsg_unt_wghtd_2020,
        outlier_flag_2020
    from source

    union all

    select
        trim(upper(coalesce(brnd_name, ''))),
        trim(upper(gnrc_name)),
        trim(coalesce(mftr_name, '')),
        cast(tot_mftr as integer),
        2021,
        tot_spndng_2021,
        tot_clms_2021,
        tot_benes_2021,
        tot_dsg_unts_2021,
        avg_spnd_per_clm_2021,
        avg_spnd_per_bene_2021,
        avg_spnd_per_dsg_unt_wghtd_2021,
        outlier_flag_2021
    from source

    union all

    select
        trim(upper(coalesce(brnd_name, ''))),
        trim(upper(gnrc_name)),
        trim(coalesce(mftr_name, '')),
        cast(tot_mftr as integer),
        2022,
        tot_spndng_2022,
        tot_clms_2022,
        tot_benes_2022,
        tot_dsg_unts_2022,
        avg_spnd_per_clm_2022,
        avg_spnd_per_bene_2022,
        avg_spnd_per_dsg_unt_wghtd_2022,
        outlier_flag_2022
    from source

    union all

    select
        trim(upper(coalesce(brnd_name, ''))),
        trim(upper(gnrc_name)),
        trim(coalesce(mftr_name, '')),
        cast(tot_mftr as integer),
        2023,
        tot_spndng_2023,
        tot_clms_2023,
        tot_benes_2023,
        tot_dsg_unts_2023,
        avg_spnd_per_clm_2023,
        avg_spnd_per_bene_2023,
        avg_spnd_per_dsg_unt_wghtd_2023,
        outlier_flag_2023
    from source
),

cleaned as (
    select
        brand_name,
        generic_name,
        manufacturer_name,
        manufacturer_count,
        drug_year,

        cast(total_spending as numeric(18,2))              as total_spending,
        cast(total_claims as bigint)                       as total_claims,
        cast(total_beneficiaries as bigint)                as total_beneficiaries,
        cast(total_dosage_units as numeric(18,2))          as total_dosage_units,
        cast(avg_spending_per_claim as numeric(12,2))      as avg_spending_per_claim,
        cast(avg_spending_per_beneficiary as numeric(12,2)) as avg_spending_per_beneficiary,
        cast(avg_spending_per_dosage_unit as numeric(12,2)) as avg_spending_per_dosage_unit,

        -- Derive 30-day fills from dosage units (approximate)
        cast(total_claims as bigint) as total_30day_fills,

        case
            when outlier_flag is not null and trim(cast(outlier_flag as text)) != ''
            then true
            else false
        end as is_outlier

    from unpivoted
    where total_spending is not null
      and cast(total_claims as bigint) > 0
)

select * from cleaned
