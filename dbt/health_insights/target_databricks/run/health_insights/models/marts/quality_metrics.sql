
  
  create or replace view `health_insights`.`marts`.`quality_metrics`
  
  as (
    

WITH f AS (
  SELECT * FROM `health_insights`.`marts`.`fato_nascimentos`
),
t AS (
  SELECT sk_tempo, data_dia
  FROM `health_insights`.`marts`.`dim_tempo`
)
SELECT
  t.data_dia AS birth_date,
  
  
    date_format(t.data_dia, 'yyyy-MM')
  
 AS ym,
  COUNT(*) AS n_registos,
  AVG(CASE WHEN f.birth_weight_g    IS NULL THEN 1 ELSE 0 END) AS pct_sem_peso,
  AVG(CASE WHEN f.gestational_weeks IS NULL THEN 1 ELSE 0 END) AS pct_sem_gestacao
FROM f
LEFT JOIN t ON f.fk_sk_tempo = t.sk_tempo
GROUP BY 1,2
  )
