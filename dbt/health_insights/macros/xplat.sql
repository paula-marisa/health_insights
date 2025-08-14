{% macro x_str(e) %}
  {% if target.type in ['databricks','spark'] %}
    cast({{ e }} as string)
  {% else %}
    cast({{ e }} as varchar)
  {% endif %}
{% endmacro %}

{% macro x_int(e) %}
  -- int funciona em Snowflake e Spark
  cast({{ e }} as int)
{% endmacro %}

{% macro x_bool(e) %}
  cast({{ e }} as boolean)
{% endmacro %}

{% macro x_date(e) %}
  {% if target.type in ['databricks','spark'] %}
    cast({{ e }} as date)
  {% else %}
    cast({{ e }} as date)
  {% endif %}
{% endmacro %}

{% macro x_regexp_like(e, pattern) %}
  {% if target.type in ['databricks','spark'] %}
    ({{ e }} rlike '{{ pattern }}')
  {% else %}
    regexp_like({{ e }}, '{{ pattern }}')
  {% endif %}
{% endmacro %}

{% macro x_only_digits(e) %}
  regexp_replace({{ e }}, '[^0-9]', '')
{% endmacro %}

{% macro x_try_to_date(e, fmt=None) %}
  {% if target.type == 'snowflake' %}
    {% if fmt %}
      try_to_date({{ e }}, '{{ fmt }}')
    {% else %}
      try_to_date({{ e }})
    {% endif %}
  {% else %}
    {% if fmt %}
      to_date({{ e }}, '{{ fmt|lower }}')
    {% else %}
      to_date({{ e }})
    {% endif %}
  {% endif %}
{% endmacro %}

{% macro x_date_ym(e) %}
  {% if target.type in ['databricks','spark'] %}
    date_format({{ e }}, 'yyyy-MM')
  {% else %}
    to_char({{ e }}, 'YYYY-MM')
  {% endif %}
{% endmacro %}

{% macro x_date_iso(e) %}
  {% if target.type in ['databricks','spark'] %}
    date_format({{ e }}, 'yyyy-MM-dd')
  {% else %}
    to_char({{ e }}, 'YYYY-MM-DD')
  {% endif %}
{% endmacro %}

{% macro x_dow_iso(e) %}
  {% if target.type in ['databricks','spark'] %}
    -- dayofweek: Dom=1 .. Sab=7 → converter para ISO (Seg=1 .. Dom=7)
    (((dayofweek({{ e }}) + 5) % 7) + 1)
  {% else %}
    dayofweekiso({{ e }})
  {% endif %}
{% endmacro %}

