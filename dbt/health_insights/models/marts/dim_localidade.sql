{{ config(materialized='table') }}

with muni as (
  select distinct municipality_code::string as municipio_code
  from {{ ref('int_births_enriched') }}
  where municipality_code is not null
),
ref as (
  -- colunas do seed CITADAS para bater no header tal como foi carregado
  select
    "municipio_code"::string as municipio_code,
    "municipio_nome"        as municipio_nome,
    "uf_sigla"              as uf_sigla,
    "regiao"                as regiao
  from {{ ref('ref_municipios') }}
)
select
  md5(m.municipio_code)        as sk_localidade,
  m.municipio_code,
  r.municipio_nome,
  r.uf_sigla,
  substr(m.municipio_code,1,2) as uf_code,
  r.regiao
from muni m
left join ref r
  on r.municipio_code = m.municipio_code
