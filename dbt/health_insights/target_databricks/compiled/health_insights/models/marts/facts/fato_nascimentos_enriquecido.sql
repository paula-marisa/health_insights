-- models/marts/fato_nascimentos_enriquecido.sql


with f as (
  select * from `health_insights`.`silver`.`int_births_enriched`
)
select
  f.*,
  d.sk_localidade,
  d.nome_municipio,
  d.state_name
from f
left join `health_insights`.`marts`.`dim_localidade` d
  on d.municipality_code = f.municipality_code