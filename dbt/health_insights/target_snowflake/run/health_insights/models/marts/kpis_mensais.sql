
  create or replace   view HEALTH_INSIGHTS.marts.kpis_mensais
  
  
  
  
  as (
    

WITH f AS (
  SELECT * FROM HEALTH_INSIGHTS.marts.fato_nascimentos
),
t AS (
  SELECT sk_tempo, 
  
    to_char(data_dia, 'YYYY-MM')
  
 AS ym
  FROM HEALTH_INSIGHTS.marts.dim_tempo
),
l AS (
  SELECT sk_localidade, state_code, state_name
  FROM HEALTH_INSIGHTS.marts.dim_localidade
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
  );

