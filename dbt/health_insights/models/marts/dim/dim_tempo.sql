{{ config(materialized='table') }}

with d as (
  select distinct {{ x_date('birth_date') }} as data
  from {{ ref('int_births_enriched') }}
  where birth_date is not null
)
select
  md5({{ x_date_iso('data') }})      as sk_tempo,
  data                                as data_dia,
  year(data)                          as ano,
  month(data)                         as mes,
  {{ x_date_ym('data') }}             as ym,
  quarter(data)                       as trimestre,
  {{ x_dow_iso('data') }}             as dia_semana_iso,
  case when {{ x_dow_iso('data') }} in (6,7) then 1 else 0 end as is_fim_semana
from d
