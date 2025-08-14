{{ config(
  materialized='view',
  schema='silver',
  create_view_as_replace=True,
  pre_hook=[
    "{% if target.type in ['databricks','spark'] %}DROP VIEW  IF EXISTS {{ target.catalog }}.{{ this.schema }}.{{ this.identifier }}{% endif %}",
    "{% if target.type in ['databricks','spark'] %}DROP TABLE IF EXISTS {{ target.catalog }}.{{ this.schema }}.{{ this.identifier }}{% endif %}",
    "{% if target.type in ['databricks','spark'] %}DROP VIEW  IF EXISTS {{ target.catalog }}.{{ this.schema }}.{{ this.identifier }}__dbt_backup{% endif %}",
    "{% if target.type in ['databricks','spark'] %}DROP TABLE IF EXISTS {{ target.catalog }}.{{ this.schema }}.{{ this.identifier }}__dbt_backup{% endif %}",
    "{% if target.type in ['databricks','spark'] %}DROP VIEW  IF EXISTS {{ target.catalog }}.{{ this.schema }}.{{ this.identifier }}__dbt_tmp{% endif %}",
    "{% if target.type in ['databricks','spark'] %}DROP TABLE IF EXISTS {{ target.catalog }}.{{ this.schema }}.{{ this.identifier }}__dbt_tmp{% endif %}"
  ]
) }}

WITH f AS (
  SELECT * FROM {{ ref('fato_nascimentos') }}
),
t AS (
  SELECT sk_tempo, {{ x_date_ym('data_dia') }} AS ym
  FROM {{ ref('dim_tempo') }}
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
