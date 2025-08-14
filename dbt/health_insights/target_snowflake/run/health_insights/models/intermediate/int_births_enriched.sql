
  create or replace   view HEALTH_INSIGHTS.silver.int_births_enriched
  
  
  
  
  as (
    

with s as (
  select * from HEALTH_INSIGHTS.raw_stg.stg_births
)
select
  s.sk_birth,
  s.municipality_code,
  s.birth_date,
  
  
    
      try_to_date(concat(s.ym, '-01'))
    
  
 as year_month_date,
  s.ym,

  -- CAMPOS QUE A FATO PRECISA:
  s.sex_newborn,
  /* ajuste os nomes conforme existirem no teu stg_births */
  s.birth_weight_g         as birth_weight_g,      -- se no stg for 'peso_nascimento_g', mapeia: s.peso_nascimento_g as birth_weight_g
  s.gestational_weeks      as gestational_weeks,   -- idem ('semanas_gestacao' -> gestational_weeks)
  s.delivery_type          as delivery_type,       -- idem ('tipo_parto' -> delivery_type)

  -- outros campos úteis
  substr(cast(s.municipality_code as string), 1, 2) as state_code
from s
  );

