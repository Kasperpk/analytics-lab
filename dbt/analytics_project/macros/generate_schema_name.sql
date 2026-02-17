{#
  This macro overrides dbt's default schema naming behavior.
  dbt calls generate_schema_name implicitly for every model/test relation,
  so explicit references are not required in model SQL files.

  Why needed in this project:
  - profile target schema is `main` (profiles.yml)
  - models set custom schemas like `bronze`, `silver`, `gold`
  - we want exact schema names (`silver`) rather than default prefixed names (`main_silver`)
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {# If a model has no custom schema, keep the target schema. #}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {# If a custom schema exists, use it directly after trimming whitespace. #}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
