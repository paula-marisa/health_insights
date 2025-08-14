
  
  create or replace view `health_insights`.`marts`.`kpis_mensais`
  
  as (
    

WITH f AS (
  SELECT * FROM `health_insights`.`marts`.`fato_nascimentos`
),
t AS (
  SELECT sk_tempo, 
  
    date_format(data_dia, 'yyyy-MM')
  
 AS ym
  FROM `health_insights`.`marts`.`dim_tempo`
),
l AS (
  SELECT sk_localidade, state_code, state_name
  FROM `health_insights`.`marts`.`dim_localidade`
)
SELECT
  t.ym,
  l.state_code,
  COUNT(*) AS total_nascimentos,
  AVG(CASE WHEN UPPER(f.delivery_type) IN ('C','CESAREA','CESÁREA') THEN 1 ELSE 0 END) AS pct_cesarianas
FROM f
JOIN t ON f.fk_sk_tempo = t.sk_tempo
JOIN l ON f.fk_sk_localidade = l.sk_localidade
GROUP BY 1,2
  )
