{{ config(materialized='view', schema='silver') }}

with f as (
  select * from {{ ref('fato_nascimentos') }}
),
t as (
  select sk_tempo, data_dia, ano, mes from {{ ref('dim_tempo') }}
)
select
    -- colunas da fato
    f.sk_birth,
    f.fk_sk_tempo,
    f.fk_sk_localidade,
    f.birth_weight_g,
    f.gestational_weeks,
    f.delivery_type,
    -- etc...

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
