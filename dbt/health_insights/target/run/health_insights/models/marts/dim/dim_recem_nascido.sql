
  
    

create or replace transient table HEALTH_INSIGHTS.RAW_STG_marts.dim_recem_nascido
    
    
    
    as (

WITH s AS (
  SELECT
    sex_newborn,
    birth_weight_g,
    gestational_weeks
  FROM HEALTH_INSIGHTS.RAW_STG_silver.int_births_enriched
)
SELECT
  md5(
    coalesce(sex_newborn,'') || '|' ||
    coalesce(
      CASE
        WHEN birth_weight_g IS NULL THEN 'desconhecido'
        WHEN birth_weight_g < 2500   THEN 'baixo_peso'
        WHEN birth_weight_g >= 4000  THEN 'macrossomico'
        ELSE 'adequado'
      END, ''
    ) || '|' ||
    coalesce(
      CASE
        WHEN gestational_weeks IS NULL THEN 'desconhecido'
        WHEN gestational_weeks < 37    THEN 'pre_termo'
        WHEN gestational_weeks BETWEEN 37 AND 41 THEN 'termo'
        WHEN gestational_weeks >= 42   THEN 'pos_termo'
      END, ''
    )
  )                                    AS sk_recem_nascido,
  sex_newborn                           AS sexo_bebe,
  CASE
    WHEN birth_weight_g IS NULL THEN 'desconhecido'
    WHEN birth_weight_g < 2500   THEN 'baixo_peso'
    WHEN birth_weight_g >= 4000  THEN 'macrossomico'
    ELSE 'adequado'
  END                                   AS categoria_peso,
  CASE
    WHEN gestational_weeks IS NULL THEN 'desconhecido'
    WHEN gestational_weeks < 37    THEN 'pre_termo'
    WHEN gestational_weeks BETWEEN 37 AND 41 THEN 'termo'
    WHEN gestational_weeks >= 42   THEN 'pos_termo'
  END                                   AS categoria_gestacao
FROM s
GROUP BY 1,2,3,4
    )
;


  