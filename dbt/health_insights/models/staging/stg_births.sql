{{ config(materialized='view') }}

{# 1) descobrir as colunas disponíveis na fonte, de forma portátil #}
{% set rel = source('raw_stg','sinasc_raw') %}
{% set cols = adapter.get_columns_in_relation(rel) %}
{% set colnames = cols | map(attribute='name') | map('lower') | list %}

with src as (
  select * from {{ rel }}
),

k as (
  {# 2) construir dinamicamente a lista para ORDER BY apenas com colunas existentes #}
  {% set order_parts = [] %}
  {% for c, kind in [
      ('DTNASC','str'),
      ('HORANASC','str'),
      ('CODESTAB','str'),
      ('CODMUNNASC','str'),
      ('SEXO','str'),
      ('PESO','int'),
      ('DTRECEBIM','str'),
      ('DTCADASTRO','str'),
      ('NUMEROLOTE','str'),
      ('CONTADOR','str')
    ] %}
    {% if c|lower in colnames %}
      {% if kind == 'int' %}
        {% do order_parts.append(x_int(c)) %}
      {% else %}
        {% do order_parts.append(x_str(c)) %}
      {% endif %}
    {% endif %}
  {% endfor %}

  select
    s.*,
    row_number() over (
      order by {{ order_parts | join(', ') if order_parts | length > 0 else '1' }}
    ) as rn
  from src s
),

norm as (
  select
    -- usa CONTADOR/CODMUNNASC se existirem; caso contrário usa NULL para estabilizar o hash
    md5(
      coalesce({% if 'contador'   in colnames %} {{ x_str('CONTADOR') }} {% else %} cast(null as string) {% endif %}, '') || '-' ||
      coalesce({% if 'codmunnasc' in colnames %} {{ x_str('CODMUNNASC') }} {% else %} cast(null as string) {% endif %}, '') || '-' ||
      lpad({{ x_str('rn') }}, 10, '0')
    ) as sk_birth,

    -- data de nascimento: várias formas
    case
      when {{ x_regexp_like(x_str('DTNASC'), '^[0-9]{4}-[0-9]{2}-[0-9]{2}$') }} then
        {{ x_try_to_date(x_str('DTNASC')) }}
      when {{ x_regexp_like("regexp_replace(" ~ x_str('DTNASC') ~ ",'[^0-9]','')", '^[0-9]{8}$') }} then
        coalesce(
          {{ x_try_to_date("regexp_replace(" ~ x_str('DTNASC') ~ ",'[^0-9]','')", 'DDMMYYYY') }},
          {{ x_try_to_date("regexp_replace(" ~ x_str('DTNASC') ~ ",'[^0-9]','')", 'YYYYMMDD') }}
        )
      when {{ x_regexp_like("regexp_replace(" ~ x_str('DTNASC') ~ ",'[^0-9]','')", '^[0-9]{7}$') }} then
        {{ x_try_to_date("lpad(regexp_replace(" ~ x_str('DTNASC') ~ ",'[^0-9]',''), 8, '0')", 'DDMMYYYY') }}
      else null
    end as birth_date,

    case {{ x_str('SEXO') }}
      when '1' then 'M'
      when '2' then 'F'
      else 'U'
    end as sex_newborn,

    {% if 'peso' in colnames %}         {{ x_int('PESO') }}       {% else %} cast(null as int)    {% endif %} as birth_weight_g,
    {% if 'gestacao' in colnames %}     {{ x_str('GESTACAO') }}   {% else %} cast(null as string) {% endif %} as gestation_code,
    {% if 'semagestac' in colnames %}   {{ x_int('SEMAGESTAC') }} {% else %} cast(null as int)    {% endif %} as gestational_weeks,
    {% if 'parto' in colnames %}        {{ x_str('PARTO') }}      {% else %} cast(null as string) {% endif %} as delivery_type,
    {% if 'codmunnasc' in colnames %}   {{ x_str('CODMUNNASC') }} {% else %} cast(null as string) {% endif %} as municipality_code
  from k
)

select
  sk_birth, birth_date, sex_newborn, birth_weight_g,
  gestation_code, gestational_weeks, delivery_type, municipality_code,
  {{ x_date_ym('birth_date') }} as ym
from norm
where birth_date is not null
