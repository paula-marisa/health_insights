

WITH s AS (
  SELECT municipality_code, ym
  FROM HEALTH_INSIGHTS.silver.int_births_enriched
)
SELECT
  -- portátil entre Snowflake e Databricks
  md5(cast(coalesce(cast(municipality_code as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(ym as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS sk_mae,
  CAST(NULL AS INT)                                                  AS idade_mae,      -- <- era NUMBER
  'desconhecido'                                                     AS faixa_etaria_mae,
  'desconhecido'                                                     AS escolaridade_mae
FROM s
GROUP BY 1,2,3,4