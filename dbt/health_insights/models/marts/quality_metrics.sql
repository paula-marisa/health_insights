{{ config(materialized='view') }}

WITH f AS (
  SELECT * FROM {{ ref('fato_nascimentos') }}
),
t AS (
  SELECT sk_tempo, data_dia
  FROM {{ ref('dim_tempo') }}
)
SELECT
  t.data_dia AS birth_date,
  {{ x_date_ym('t.data_dia') }} AS ym,
  COUNT(*) AS n_registos,
  AVG(CASE WHEN f.birth_weight_g    IS NULL THEN 1 ELSE 0 END) AS pct_sem_peso,
  AVG(CASE WHEN f.gestational_weeks IS NULL THEN 1 ELSE 0 END) AS pct_sem_gestacao
FROM f
LEFT JOIN t ON f.fk_sk_tempo = t.sk_tempo
GROUP BY 1,2
