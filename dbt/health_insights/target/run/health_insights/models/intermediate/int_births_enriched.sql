
  create or replace   view HEALTH_INSIGHTS.RAW_STG_silver.int_births_enriched
  
  
  
  
  as (
    

WITH s AS (
  SELECT * FROM HEALTH_INSIGHTS.RAW_STG_raw_stg.stg_births
)
SELECT
  s.sk_birth,
  s.municipality_code,

  s.birth_date,
  
  
    
      try_to_date(concat(s.ym, '-01'))
    
  
 AS year_month_date,  -- YYYY-MM-01
  s.ym,

  s.sex_newborn,
  s.birth_weight_g,
  s.gestation_code,
  s.gestational_weeks,
  s.delivery_type,

  substr(cast(s.municipality_code as string), 1, 2) AS state_code,

  CASE
    WHEN s.birth_weight_g IS NULL THEN 'desconhecido'
    WHEN s.birth_weight_g < 2500   THEN 'baixo_peso'
    WHEN s.birth_weight_g >= 4000  THEN 'macrossomico'
    ELSE 'adequado'
  END AS birth_weight_category,

  CASE
    WHEN s.gestational_weeks IS NULL THEN 'desconhecido'
    WHEN s.gestational_weeks < 37    THEN 'pre_termo'
    WHEN s.gestational_weeks BETWEEN 37 AND 41 THEN 'termo'
    WHEN s.gestational_weeks >= 42   THEN 'pos_termo'
  END AS gestation_category
FROM s
  );

