-- stg_prescribers.sql
-- Staging model for CMS Medicare Part D Prescribers by Provider
-- Grain: one row per prescriber (NPI) per year
-- Source: data.cms.gov MUP_DPR (provider summary)

with source as (
    select * from {{ source('raw', 'raw_prescribers') }}
),

renamed as (
    select
        -- Provider identification
        cast(prscrbr_npi as varchar(10))                as npi,
        trim(prscrbr_last_org_name)                     as last_name_or_org,
        trim(coalesce(prscrbr_first_name, ''))          as first_name,
        trim(coalesce(prscrbr_crdntls, ''))             as credentials,
        trim(prscrbr_ent_cd)                            as entity_code,
        -- Entity: I = Individual, O = Organization

        -- Provider demographics
        trim(prscrbr_type)                              as specialty,
        trim(prscrbr_state_abrvtn)                      as state,
        trim(coalesce(prscrbr_city, ''))                as city,

        -- Utilization
        cast(tot_clms as bigint)                        as total_claims,
        cast(tot_benes as integer)                      as total_beneficiaries,
        cast(tot_drug_cst as numeric(18,2))             as total_drug_cost,
        cast(tot_30day_fills as numeric(12,2))          as total_30day_fills,

        -- Brand vs generic
        cast(brnd_tot_clms as bigint)                   as brand_claims,
        cast(gnrc_tot_clms as bigint)                   as generic_claims,
        cast(brnd_drug_cst as numeric(18,2))            as brand_drug_cost,
        cast(gnrc_drug_cst as numeric(18,2))            as generic_drug_cost,

        -- Risk indicators
        cast(opioid_tot_clms as bigint)                 as opioid_claims,
        cast(opioid_bene_cnt as integer)                as opioid_beneficiaries,
        cast(antbtc_tot_clms as bigint)                 as antibiotic_claims,

        -- Beneficiary demographics
        cast(bene_avg_age as numeric(5,1))              as avg_beneficiary_age,
        cast(bene_feml_cnt as integer)                  as female_beneficiary_count,
        cast(bene_male_cnt as integer)                  as male_beneficiary_count,

        2023 as prescriber_year

    from source
    where prscrbr_npi is not null
)

select * from renamed
