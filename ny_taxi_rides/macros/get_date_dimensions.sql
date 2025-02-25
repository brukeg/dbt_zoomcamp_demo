{#
    This macro returns several date dimensions 
#}

{% macro get_date_dimensions(timestamp_column) %}
    EXTRACT(YEAR FROM {{ timestamp_column }}) AS year,
    EXTRACT(QUARTER FROM {{ timestamp_column }}) AS quarter,
    CONCAT(EXTRACT(YEAR FROM {{ timestamp_column }}), '/Q', EXTRACT(QUARTER FROM {{ timestamp_column }})) AS year_quarter,
    EXTRACT(MONTH FROM {{ timestamp_column }}) AS month
{%- endmacro %}