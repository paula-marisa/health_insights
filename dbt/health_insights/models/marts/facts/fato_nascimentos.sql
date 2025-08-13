{{ config(
    materialized='incremental',
    unique_key='sk_birth',
    on_schema_change='sync_all_columns'
) }}

with src as (
  select
    sk_birth,
    birth_date,
    sex_newborn,
    birth_weight_g,
    gestational_weeks,
    municipality_code,
    -- categorias alinhadas com a dim_recem_nascido
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
  from {{ ref('int_births_enriched') }}
  {% if is_incremental() %}
    -- janela incremental baseada na data (última data já carregada)
    where birth_date >= (
      select coalesce(max(t.data_dia),'1900-01-01'::date)
      from {{ this }} f
      join {{ ref('dim_tempo') }} t on f.fk_sk_tempo = t.sk_tempo
    )
  {% endif %}
),

lk_tempo as (
  select sk_tempo, data_dia
  from {{ ref('dim_tempo') }}
),

lk_localidade as (
  -- atenção: na tua dimensão o campo chama-se municipio_code (pt),
  -- no int chama-se municipality_code (en)
  select sk_localidade, municipio_code
  from {{ ref('dim_localidade') }}
),

lk_recem as (
  select sk_recem_nascido, sexo_bebe, categoria_peso, categoria_gestacao
  from {{ ref('dim_recem_nascido') }}
)

select
  s.sk_birth,
  t.sk_tempo              as fk_sk_tempo,
  dloc.sk_localidade      as fk_sk_localidade,
  drec.sk_recem_nascido   as fk_sk_recem_nascido,
  s.sex_newborn
from src s
left join lk_tempo      t    on s.birth_date = t.data_dia
left join lk_localidade dloc on s.municipality_code = dloc.municipio_code
left join lk_recem      drec
       on drec.sexo_bebe = s.sex_newborn
      and drec.categoria_peso = s.categoria_peso
      and drec.categoria_gestacao = s.categoria_gestacao
