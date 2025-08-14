{% macro as_float(expr) -%}
  {# Converte para tipo float adequado ao adaptador #}
  {%- if target.type in ['databricks', 'spark'] -%}
    CAST({{ expr }} AS DOUBLE)
  {%- elif target.type == 'snowflake' -%}
    CAST({{ expr }} AS FLOAT)
  {%- else -%}
    CAST({{ expr }} AS DOUBLE PRECISION)
  {%- endif -%}
{%- endmacro %}

{% macro safe_ratio(numerator, denominator) -%}
  {# Evita divisão por zero em qualquer engine #}
  CASE WHEN {{ denominator }} = 0 THEN NULL ELSE ({{ numerator }} / {{ denominator }}) END
{%- endmacro %}
