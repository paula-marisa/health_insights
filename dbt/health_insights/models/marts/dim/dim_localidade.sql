-- models/marts/dim/dim_localidade.sql
{{ config(materialized='table') }}

with muni as (
  select
    municipio_code,              -- vem do seed
    uf_sigla       as state_code,
    municipio_nome,
    regiao
  from {{ ref('ref_municipios') }}
),
ufs as (
  select
    uf_sigla as state_code,
    uf_nome  as state_name
  from {{ ref('ref_uf') }}
)
select
  {{ dbt_utils.generate_surrogate_key(['m.municipio_code']) }} as sk_localidade,
  m.municipio_code  as municipality_code,
  m.state_code,
  u.state_name,
  m.municipio_nome  as nome_municipio,
  m.regiao
from muni m
left join ufs u using (state_code)