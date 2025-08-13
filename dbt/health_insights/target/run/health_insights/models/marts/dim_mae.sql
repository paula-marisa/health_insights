
  
    

create or replace transient table HEALTH_INSIGHTS.RAW_STG_marts.dim_mae
    
    
    
    as (

with src as (
  select distinct
    idademae::int        as idade_mae,
    escmae::string       as escolaridade_mae,
    estcivmae::string    as estado_civil_mae,
    racacor::string      as raca_cor_mae,
    qtnfilhosvivos::int  as filhos_vivos,
    qtnfilhosmortos::int as filhos_mortos
  from HEALTH_INSIGHTS.RAW_STG_silver.int_births_enriched
)
select
  md5(
    coalesce(to_varchar(idade_mae),'') || '|' ||
    coalesce(escolaridade_mae,'')      || '|' ||
    coalesce(estado_civil_mae,'')      || '|' ||
    coalesce(raca_cor_mae,'')          || '|' ||
    coalesce(to_varchar(filhos_vivos),'') || '|' ||
    coalesce(to_varchar(filhos_mortos),'')
  ) as sk_mae,
  idade_mae,
  escolaridade_mae,
  estado_civil_mae,
  raca_cor_mae,
  filhos_vivos,
  filhos_mortos
from src
    )
;


  