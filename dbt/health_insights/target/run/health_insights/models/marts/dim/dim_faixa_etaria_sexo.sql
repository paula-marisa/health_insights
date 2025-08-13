
  
    

create or replace transient table HEALTH_INSIGHTS.RAW_STG_marts.dim_faixa_etaria_sexo
    
    
    
    as (

with src as (
  select distinct
    idademae::int        as idade_mae,
    sex_newborn::string  as sexo_bebe
  from HEALTH_INSIGHTS.RAW_STG_silver.int_births_enriched
  where sex_newborn is not null
)
select
  md5(
    coalesce(
      case
        when idade_mae is null then 'desconhecida'
        when idade_mae < 15 then '<15'
        when idade_mae between 15 and 19 then '15-19'
        when idade_mae between 20 and 24 then '20-24'
        when idade_mae between 25 and 29 then '25-29'
        when idade_mae between 30 and 34 then '30-34'
        when idade_mae between 35 and 39 then '35-39'
        when idade_mae between 40 and 44 then '40-44'
        else '45+'
      end, ''
    ) || '|' || coalesce(sexo_bebe,'')
  ) as sk_faixa_etaria_sexo,
  case
    when idade_mae is null then 'desconhecida'
    when idade_mae < 15 then '<15'
    when idade_mae between 15 and 19 then '15-19'
    when idade_mae between 20 and 24 then '20-24'
    when idade_mae between 25 and 29 then '25-29'
    when idade_mae between 30 and 34 then '30-34'
    when idade_mae between 35 and 39 then '35-39'
    when idade_mae between 40 and 44 then '40-44'
    else '45+'
  end as faixa_etaria_mae,
  sexo_bebe
from src
    )
;


  