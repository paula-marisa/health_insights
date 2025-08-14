

WITH s AS (
  SELECT municipality_code, ym
  FROM `health_insights`.`silver`.`int_births_enriched`
)
SELECT
  -- portátil entre Snowflake e Databricks
  md5(cast(concat(coalesce(cast(municipality_code as string), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(ym as string), '_dbt_utils_surrogate_key_null_')) as string)) AS sk_mae,
  CAST(NULL AS INT)                                                  AS idade_mae,      -- <- era NUMBER
  'desconhecido'                                                     AS faixa_etaria_mae,
  'desconhecido'                                                     AS escolaridade_mae
FROM s
GROUP BY 1,2,3,4