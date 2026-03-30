# Looker LookML Model — Pharmacy Operations Analytics
#
# This LookML model demonstrates how to connect the dbt mart layer
# to Looker for self-service analytics. It is a reference artifact,
# not a deployed Looker instance.
#
# Connection assumes Snowflake target (prod_snowflake in profiles.yml).

connection: "pharma_ops_snowflake"

# ── Explores ─────────────────────────────────────────────────

explore: drug_spending {
  label: "Drug Spending Analysis"
  description: "Explore drug spending trends, specialty vs generic, and Pareto analysis."

  join: therapeutic_area_kpis {
    type: left_outer
    relationship: many_to_one
    sql_on: ${drug_spending.therapeutic_area} = ${therapeutic_area_kpis.therapeutic_area}
            AND ${drug_spending.drug_year} = ${therapeutic_area_kpis.drug_year} ;;
  }

  access_filter: {
    field: drug_spending.therapeutic_area
    user_attribute: allowed_therapeutic_areas
  }
}

explore: prescriber_engagement {
  label: "Prescriber Engagement"
  description: "Prescriber engagement tiers, specialty adoption, and geographic distribution."

  join: geographic_distribution {
    type: left_outer
    relationship: many_to_one
    sql_on: ${prescriber_engagement.state} = ${geographic_distribution.state}
            AND ${prescriber_engagement.prescriber_year} = ${geographic_distribution.drug_year} ;;
  }
}

explore: pharmacy_operations {
  label: "Pharmacy Operations"
  description: "Fill volumes, cost per claim, day supply efficiency, brand/generic ratios."
}

# ── Views ────────────────────────────────────────────────────

view: drug_spending {
  sql_table_name: marts.mart_cost_per_claim ;;

  dimension: cost_claim_key {
    primary_key: yes
    type: string
    sql: ${TABLE}.cost_claim_key ;;
  }

  dimension: drug_year {
    type: number
    sql: ${TABLE}.drug_year ;;
  }

  dimension: generic_name {
    type: string
    sql: ${TABLE}.generic_name ;;
  }

  dimension: brand_name {
    type: string
    sql: ${TABLE}.brand_name ;;
  }

  dimension: therapeutic_area {
    type: string
    sql: ${TABLE}.therapeutic_area ;;
  }

  dimension: is_specialty_drug {
    type: yesno
    sql: ${TABLE}.is_specialty_drug ;;
  }

  dimension: brand_generic_flag {
    type: string
    sql: ${TABLE}.brand_generic_flag ;;
  }

  # ── Measures ───────────────────────────────────────────────

  measure: total_spending {
    type: sum
    sql: ${TABLE}.total_spending ;;
    value_format_name: usd
  }

  measure: total_claims {
    type: sum
    sql: ${TABLE}.total_claims ;;
    value_format_name: decimal_0
  }

  measure: avg_cost_per_claim {
    type: average
    sql: ${TABLE}.cost_per_claim ;;
    value_format_name: usd
  }

  measure: avg_monthly_cost {
    type: average
    sql: ${TABLE}.cost_per_beneficiary_monthly ;;
    value_format_name: usd
  }

  measure: unique_drugs {
    type: count_distinct
    sql: ${TABLE}.generic_name ;;
  }
}

view: therapeutic_area_kpis {
  sql_table_name: marts.mart_therapeutic_area_kpis ;;

  dimension: ta_kpi_key {
    primary_key: yes
    type: string
    sql: ${TABLE}.ta_kpi_key ;;
  }

  dimension: therapeutic_area {
    type: string
    sql: ${TABLE}.therapeutic_area ;;
  }

  dimension: drug_year {
    type: number
    sql: ${TABLE}.drug_year ;;
  }

  measure: total_spending {
    type: sum
    sql: ${TABLE}.total_spending ;;
    value_format_name: usd
  }

  measure: drug_count {
    type: sum
    sql: ${TABLE}.drug_count ;;
  }

  measure: specialty_spending_pct {
    type: average
    sql: ${TABLE}.specialty_spending_pct ;;
    value_format: "0.0\"%\""
  }
}

view: prescriber_engagement {
  sql_table_name: marts.mart_prescriber_engagement ;;

  dimension: prescriber_key {
    primary_key: yes
    type: string
    sql: ${TABLE}.prescriber_key ;;
  }

  dimension: engagement_tier {
    type: string
    sql: ${TABLE}.engagement_tier ;;
  }

  dimension: specialty {
    type: string
    sql: ${TABLE}.specialty ;;
  }

  dimension: state {
    type: string
    map_layer_name: us_states
    sql: ${TABLE}.state ;;
  }

  measure: prescriber_count {
    type: count_distinct
    sql: ${TABLE}.npi ;;
  }

  measure: avg_brand_rate {
    type: average
    sql: ${TABLE}.brand_prescribing_rate_pct ;;
    value_format: "0.0\"%\""
  }
}

view: geographic_distribution {
  sql_table_name: marts.mart_geographic_distribution ;;

  dimension: geo_key {
    primary_key: yes
    type: string
    sql: ${TABLE}.geo_key ;;
  }

  dimension: state {
    type: string
    map_layer_name: us_states
    sql: ${TABLE}.state ;;
  }

  dimension: drug_year {
    type: number
    sql: ${TABLE}.drug_year ;;
  }

  measure: total_drug_cost {
    type: sum
    sql: ${TABLE}.total_drug_cost ;;
    value_format_name: usd
  }

  measure: prescriber_count {
    type: sum
    sql: ${TABLE}.prescriber_count ;;
  }
}
