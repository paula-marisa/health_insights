
  
    

create or replace transient table HEALTH_INSIGHTS.RAW_STG_marts.dim_faixa_etaria_sexo
    
    
    
    as (

WITH s AS (
  SELECT DISTINCT
    sex_newborn
  FROM HEALTH_INSIGHTS.RAW_STG_silver.int_births_enriched
)
SELECT
  md5(coalesce(sex_newborn,'') || '|desconhecido') AS sk_faixa_etaria_sexo,
  'desconhecido'                                   AS faixa_etaria_mae,
  sex_newborn                                      AS sexo_bebe
FROM s
    )
;


  