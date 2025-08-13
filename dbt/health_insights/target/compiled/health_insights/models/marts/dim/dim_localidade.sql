

with muni as (
  select distinct municipality_code::string as municipio_code
  from HEALTH_INSIGHTS.RAW_STG_silver.int_births_enriched
  where municipality_code is not null
),
ref as (
  select
    MUNICIPIO_CODE::string as municipio_code,
    MUNICIPIO_NOME::string as municipio_nome,
    UF_SIGLA::string       as uf_sigla,
    REGIAO::string         as regiao
  from HEALTH_INSIGHTS.RAW_STG_RAW_STG.ref_municipios
)
select
  md5(m.municipio_code)        as sk_localidade,
  m.municipio_code,
  coalesce(r.municipio_nome, 'desconhecido') as municipio_nome,
  coalesce(r.uf_sigla, substr(m.municipio_code,1,2)) as uf_sigla,
  coalesce(r.regiao, 'desconhecida') as regiao
from muni m
left join ref r on r.municipio_code = m.municipio_code