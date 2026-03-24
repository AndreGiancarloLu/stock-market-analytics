{% macro get_daily_return(open, close) -%}
    ({{ close }} - {{ open }}) / NULLIF({{ open }}, 0)
{%- endmacro %}