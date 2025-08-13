{% macro pick_col(model_name, candidates) -%}
  {%- set cols = adapter.get_columns_in_relation(ref(model_name)) -%}
  {%- set names = cols | map(attribute='name') | map('lower') | list -%}
  {%- for c in candidates -%}
    {%- if c is string and c|lower in names -%}
      {{ return(c) }}
    {%- endif -%}
  {%- endfor -%}
  null
{%- endmacro %}
