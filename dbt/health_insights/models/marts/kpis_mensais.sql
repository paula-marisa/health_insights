{{ config(
  materialized='view',
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
),
l AS (
  SELECT sk_localidade, state_code, state_name
  FROM {{ ref('dim_localidade') }}
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
