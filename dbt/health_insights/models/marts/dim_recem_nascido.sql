-- models/marts/dim_recem_nascido.sql
{{ config(materialized='table') }}

with src as (
  select distinct
    sex_newborn::string   as sexo,
    birth_weight_g::int   as peso_g,
    is_low_weight::boolean  as is_baixo_peso,
    is_premature::boolean   as is_prematuro,
    is_cesarean::boolean    as is_cesarea
  from {{ ref('int_births_enriched') }}
),
faixas as (
  select
    sexo,
    peso_g,
    case
      when peso_g is null then 'desconhecido'
      when peso_g < 2500 then '<2.5kg'
      when peso_g between 2500 and 3999 then '2.5-3.9kg'
      when peso_g >= 4000 then '>=4.0kg'
    end as faixa_peso,
    is_baixo_peso,
    is_prematuro,
    is_cesarea
  from src
)
select
  md5(coalesce(sexo,'') || '|' || coalesce(faixa_peso,'') || '|' ||
      coalesce(is_baixo_peso::string,'') || '|' ||
      coalesce(is_prematuro::string,'') || '|' ||
      coalesce(is_cesarea::string,''))     as sk_recem_nascido,
  sexo,
  faixa_peso,
  is_baixo_peso,
  is_prematuro,
  is_cesarea
from faixas
