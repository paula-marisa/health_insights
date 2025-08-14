{{ config(
  materialized='view',
  schema='silver',
  create_view_as_replace=True,
  pre_hook=[
    "{% if target.type in ['databricks','spark'] %}DROP VIEW  IF EXISTS {{ target.catalog }}.{{ this.schema }}.{{ this.identifier }}{% endif %}",
    "{% if target.type in ['databricks','spark'] %}DROP TABLE IF EXISTS {{ target.catalog }}.{{ this.schema }}.{{ this.identifier }}{% endif %}",
    "{% if target.type in ['databricks','spark'] %}DROP VIEW  IF EXISTS {{ target.catalog }}.{{ this.schema }}.{{ this.identifier }}__dbt_backup{% endif %}",
    "{% if target.type in ['databricks','spark'] %}DROP TABLE IF EXISTS {{ target.catalog }}.{{ this.schema }}.{{ this.identifier }}__dbt_backup{% endif %}",
    "{% if target.type in ['databricks','spark'] %}DROP VIEW  IF EXISTS {{ target.catalog }}.{{ this.schema }}.{{ this.identifier }}__dbt_tmp{% endif %}",
    "{% if target.type in ['databricks','spark'] %}DROP TABLE IF EXISTS {{ target.catalog }}.{{ this.schema }}.{{ this.identifier }}__dbt_tmp{% endif %}"
  ]
) }}

with f as (
  select * from {{ ref('fato_nascimentos') }}
),
t as (
  select sk_tempo, data_dia, ano, mes from {{ ref('dim_tempo') }}
)
select
  -- colunas necessárias da fato
  f.sk_birth,
  f.fk_sk_tempo,
  f.fk_sk_localidade,
  f.birth_weight_g,
  f.gestational_weeks,
  f.delivery_type,

  -- birth_date (compatível)
  {% if target.type in ['databricks','spark'] %}
    t.data_dia as birth_date,
  {% else %}
    coalesce(f.birth_date, t.data_dia) as birth_date,
  {% endif %}

  -- ym canónico yyyy-MM
  {% if target.type in ['databricks','spark'] %}
    lpad(cast(t.ano as string), 4, '0') || '-' || lpad(cast(t.mes as string), 2, '0') as ym
  {% else %}
    to_char(to_date(t.ano||'-'||lpad(cast(t.mes as string),2,'0')||'-01'), 'YYYY-MM') as ym
  {% endif %}
from f
left join t
  on f.fk_sk_tempo = t.sk_tempo
