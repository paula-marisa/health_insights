
  
    

create or replace transient table HEALTH_INSIGHTS.marts.dim_mae
    
    
    
    as (

WITH s AS (
  SELECT
    municipality_code,
    ym
  FROM HEALTH_INSIGHTS.silver.int_births_enriched
)
SELECT
  md5(coalesce(municipality_code::string,'') || '|' || coalesce(ym,'')) AS sk_mae,
  CAST(NULL AS NUMBER)                                                  AS idade_mae,
  'desconhecido'                                                        AS faixa_etaria_mae,
  'desconhecido'                                                        AS escolaridade_mae
FROM s
GROUP BY 1,2,3,4
    )
;


  