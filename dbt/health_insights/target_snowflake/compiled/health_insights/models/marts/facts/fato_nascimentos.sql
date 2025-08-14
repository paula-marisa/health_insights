

with src as (
  select
    sk_birth,
    birth_date,
    sex_newborn,
    birth_weight_g,
    gestational_weeks,
    delivery_type,
    municipality_code,
    -- categorias para link com dim_recem_nascido
    case
      when birth_weight_g is null then 'desconhecido'
      when birth_weight_g < 2500   then 'baixo_peso'
      when birth_weight_g >= 4000  then 'macrossomico'
      else 'adequado'
    end as categoria_peso,
    case
      when gestational_weeks is null then 'desconhecido'
      when gestational_weeks < 37    then 'pre_termo'
      when gestational_weeks between 37 and 41 then 'termo'
      when gestational_weeks >= 42   then 'pos_termo'
    end as categoria_gestacao
  from HEALTH_INSIGHTS.silver.int_births_enriched
  
    where birth_date >= (
      select coalesce(max(t.data_dia),'1900-01-01'::date)
      from HEALTH_INSIGHTS.marts.fato_nascimentos f
      join HEALTH_INSIGHTS.marts.dim_tempo t on f.fk_sk_tempo = t.sk_tempo
    )
  
),

lk_tempo as (
  select sk_tempo, data_dia
  from HEALTH_INSIGHTS.marts.dim_tempo
),

lk_localidade as (
  select sk_localidade, municipality_code
  from HEALTH_INSIGHTS.marts.dim_localidade
),

lk_recem as (
  select sk_recem_nascido, sexo_bebe, categoria_peso, categoria_gestacao
  from HEALTH_INSIGHTS.marts.dim_recem_nascido
)

select
  -- chaves
  s.sk_birth,
  t.sk_tempo            as fk_sk_tempo,
  dloc.sk_localidade    as fk_sk_localidade,
  drec.sk_recem_nascido as fk_sk_recem_nascido,

  -- MEDIDAS / ATRIBUTOS para o dashboard
  s.sex_newborn,
  s.birth_weight_g,
  s.gestational_weeks,
  s.delivery_type
from src s
left join lk_tempo      t    on s.birth_date = t.data_dia
left join lk_localidade dloc on s.municipality_code = dloc.municipality_code
left join lk_recem      drec
       on drec.sexo_bebe = s.sex_newborn
      and drec.categoria_peso = s.categoria_peso
      and drec.categoria_gestacao = s.categoria_gestacao