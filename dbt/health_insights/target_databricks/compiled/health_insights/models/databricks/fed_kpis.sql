

WITH f AS (
  SELECT * FROM `health_insights`.`marts`.`fato_nascimentos`
),
t AS (
  SELECT sk_tempo, 
  
    date_format(data_dia, 'yyyy-MM')
  
 AS ym
  FROM `health_insights`.`marts`.`dim_tempo`
)
SELECT
  t.ym,
  COUNT(*) AS total_nascimentos,
  AVG(CASE WHEN f.gestational_weeks IS NOT NULL AND f.gestational_weeks < 37 THEN 1 ELSE 0 END) AS pct_prematuros,
  AVG(CASE WHEN f.birth_weight_g    IS NOT NULL AND f.birth_weight_g    < 2500 THEN 1 ELSE 0 END) AS pct_baixo_peso,
  AVG(CASE WHEN UPPER(f.delivery_type) IN ('C','CESAREA','CESÁREA','CESAREAN','CESARIANA') THEN 1 ELSE 0 END) AS pct_cesarianas
FROM f
JOIN t ON f.fk_sk_tempo = t.sk_tempo
GROUP BY 1