{{ config(materialized='table') }}

WITH s AS (
  SELECT municipality_code, ym
  FROM {{ ref('int_births_enriched') }}
)
SELECT
  -- portátil entre Snowflake e Databricks
  {{ dbt_utils.generate_surrogate_key(['municipality_code','ym']) }} AS sk_mae,
  CAST(NULL AS INT)                                                  AS idade_mae,      -- <- era NUMBER
  'desconhecido'                                                     AS faixa_etaria_mae,
  'desconhecido'                                                     AS escolaridade_mae
FROM s
GROUP BY 1,2,3,4
