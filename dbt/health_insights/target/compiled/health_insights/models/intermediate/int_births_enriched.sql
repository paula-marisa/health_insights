

with s as (
  select * from HEALTH_INSIGHTS.RAW_STG_raw_stg.stg_births
),
enriched as (
  select
    -- campos normalizados vindos do staging
    s.birth_date::date                                     as birth_date,
    to_char(s.birth_date, 'YYYY-MM')                       as ym,
    s.sex_newborn::string                                  as sex_newborn,
    s.birth_weight_g::int                                  as birth_weight_g,
    s.gestation_code::string                               as gestation_code,
    s.gestational_weeks::int                               as gestational_weeks,
    s.delivery_type::string                                as delivery_type,
    s.municipality_code::string                            as municipality_code,

    -- flags derivadas (agora definidas aqui)
    case when s.birth_weight_g is not null and s.birth_weight_g < 2500 then true else false end as is_low_weight,
    case when s.gestational_weeks is not null and s.gestational_weeks < 37 then true else false end as is_premature,
    case 
      when upper(coalesce(s.delivery_type,'')) like '%CES%' then true
      when s.delivery_type in ('2','CESAREA','CESÁREA') then true
      else false
    end as is_cesarean,

    -- campos maternos normalizados (se existirem no staging; senão ficam null)
    try_cast(null as int)                    as idademae,
    null::string          as escmae,
    null::string                    as estcivmae,
    null::string             as racacor,
    try_cast(null as int)          as qtnfilhosvivos,
    try_cast(null as int)         as qtnfilhosmortos,

    -- chave do nascimento (mantemos do staging)
    s.sk_birth
  from s
  where s.birth_date is not null
)
select * from enriched