-- models/marts/dim/dim_localidade.sql


with muni as (
  select
    municipio_code,              -- vem do seed
    uf_sigla       as state_code,
    municipio_nome,
    regiao
  from HEALTH_INSIGHTS.RAW_STG.ref_municipios
),
ufs as (
  select
    uf_sigla as state_code,
    uf_nome  as state_name
  from HEALTH_INSIGHTS.RAW_STG.ref_uf
)
select
  md5(cast(coalesce(cast(m.municipio_code as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as sk_localidade,
  m.municipio_code  as municipality_code,
  m.state_code,
  u.state_name,
  m.municipio_nome  as nome_municipio,
  m.regiao
from muni m
left join ufs u using (state_code)