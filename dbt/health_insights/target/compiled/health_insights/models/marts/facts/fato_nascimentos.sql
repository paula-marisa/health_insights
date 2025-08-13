

with f as (
  select
    sk_birth,
    birth_date::date       as birth_date,
    ym,
    sex_newborn,
    birth_weight_g,
    is_low_weight,
    is_premature,
    is_cesarean,
    municipality_code
  from HEALTH_INSIGHTS.RAW_STG_silver.int_births_enriched
  where birth_date between to_date('2022-01-01')
                      and to_date('2023-12-31')

  
    and birth_date >= (
      select coalesce(max(birth_date), '1900-01-01'::date)
      from HEALTH_INSIGHTS.RAW_STG_marts.fato_nascimentos
    )
  
),

lk_tempo as (
  select sk_tempo, data_dia from HEALTH_INSIGHTS.RAW_STG_marts.dim_tempo
),
lk_loc as (
  select sk_localidade, municipio_code from HEALTH_INSIGHTS.RAW_STG_marts.dim_localidade
),
lk_baby as (
  select sk_recem_nascido, sexo, faixa_peso, is_baixo_peso, is_prematuro, is_cesarea
  from HEALTH_INSIGHTS.RAW_STG_marts.dim_recem_nascido
)

select
  f.sk_birth,
  t.sk_tempo          as fk_sk_tempo,
  l.sk_localidade     as fk_sk_localidade,
  b.sk_recem_nascido  as fk_sk_recem_nascido,

  -- atributos degenerados
  f.birth_date,
  f.ym,
  f.sex_newborn,
  f.birth_weight_g,
  f.is_low_weight,
  f.is_premature,
  f.is_cesarean,
  f.municipality_code
from f
left join lk_tempo t on t.data_dia = f.birth_date
left join lk_loc   l on l.municipio_code = f.municipality_code::string
left join lk_baby  b 
  on b.sexo = f.sex_newborn
 and b.faixa_peso = case
        when f.birth_weight_g is null then 'desconhecido'
        when f.birth_weight_g < 2500 then '<2.5kg'
        when f.birth_weight_g between 2500 and 3999 then '2.5-3.9kg'
        when f.birth_weight_g >= 4000 then '>=4.0kg'
     end
 and b.is_baixo_peso = f.is_low_weight
 and b.is_prematuro  = f.is_premature
 and b.is_cesarea    = f.is_cesarean