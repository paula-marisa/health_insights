

with base as (
  select distinct
    sex_newborn::string        as sexo_raw,
    birth_weight_g::int        as peso_g_raw,
    is_low_weight::boolean     as is_baixo_peso_raw,
    is_premature::boolean      as is_prematuro_raw,
    is_cesarean::boolean       as is_cesarea_raw
  from HEALTH_INSIGHTS.RAW_STG_silver.int_births_enriched
),
norm as (
  select
    -- sexo normalizado
    case upper(coalesce(sexo_raw, 'U'))
      when 'M' then 'M'
      when 'F' then 'F'
      else 'U'
    end as sexo,

    -- faixa de peso determinística
    case
      when peso_g_raw is null then 'desconhecido'
      when peso_g_raw < 2500 then '<2.5kg'
      when peso_g_raw between 2500 and 3999 then '2.5-3.9kg'
      when peso_g_raw >= 4000 then '>=4.0kg'
    end as faixa_peso,

    -- flags com defaults
    coalesce(is_baixo_peso_raw, false) as is_baixo_peso,
    coalesce(is_prematuro_raw, false)  as is_prematuro,
    coalesce(is_cesarea_raw, false)    as is_cesarea
  from base
),
agg as (
  -- garantir unicidade das combinações
  select sexo, faixa_peso, is_baixo_peso, is_prematuro, is_cesarea
  from norm
  group by 1,2,3,4,5
)
select
  md5(
    coalesce(sexo,'') || '|' ||
    coalesce(faixa_peso,'') || '|' ||
    is_baixo_peso::string || '|' ||
    is_prematuro::string  || '|' ||
    is_cesarea::string
  ) as sk_recem_nascido,
  sexo,
  faixa_peso,
  is_baixo_peso,
  is_prematuro,
  is_cesarea
from agg