{#
  Use the custom schema name VERBATIM instead of dbt's default
  `<target_schema>_<custom_schema>` concatenation. So `+schema: usda_staging` lands models
  in a dataset literally named `usda_staging` (not `usda_analytics_usda_staging`), giving the
  clean dataset names usda_staging / usda_analytics. Nodes WITHOUT a +schema fall back to the
  profile's `dataset` (target.schema).
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
