{% macro suppress_small_cells(column, threshold=var('min_beneficiary_threshold')) %}
{#
    CMS data suppression rule: any cell with fewer than 11 beneficiaries
    must be suppressed to prevent re-identification.
    This macro replaces values below threshold with null.
#}
    case
        when {{ column }} < {{ threshold }} then null
        else {{ column }}
    end
{% endmacro %}
