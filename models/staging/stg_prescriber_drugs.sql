-- stg_prescriber_drugs.sql
-- Staging model for CMS Medicare Part D Prescribers by Provider and Drug
-- Grain: one row per prescriber per drug per year
-- Source: data.cms.gov MUP_DPR (provider-drug detail)
-- Note: Filtered during ingestion to target therapeutic areas

with source as (
    select * from {{ source('raw', 'raw_prescribers_drug') }}
),

renamed as (
    select
        -- Provider identification
        cast(prscrbr_npi as varchar(10))               as npi,
        trim(prscrbr_last_org_name)                    as last_name_or_org,
        trim(coalesce(prscrbr_first_name, ''))         as first_name,
        trim(prscrbr_type)                             as specialty,
        trim(prscrbr_state_abrvtn)                     as state,
        trim(coalesce(prscrbr_city, ''))               as city,

        -- Drug identification
        trim(upper(coalesce(brnd_name, '')))           as brand_name,
        trim(upper(gnrc_name))                         as generic_name,

        -- Utilization
        cast(tot_clms as bigint)                       as total_claims,
        cast(tot_benes as integer)                     as total_beneficiaries,
        cast(tot_day_suply as bigint)                  as total_day_supply,
        cast(tot_30day_fills as numeric(12,2))         as total_30day_fills,
        cast(tot_drug_cst as numeric(18,2))            as total_drug_cost,

        -- Suppression
        case
            when ge65_sprsn_flag is not null
                and trim(ge65_sprsn_flag) = '*'
            then true
            else false
        end as is_ge65_suppressed,

        2023 as prescriber_drug_year

    from source
    where gnrc_name is not null
      and tot_clms is not null
)

select * from renamed
