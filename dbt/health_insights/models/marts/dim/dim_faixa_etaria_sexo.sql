{{ config(materialized='table') }}

WITH s AS (
  SELECT DISTINCT
    sex_newborn
  FROM {{ ref('int_births_enriched') }}
)
SELECT
  md5(coalesce(sex_newborn,'') || '|desconhecido') AS sk_faixa_etaria_sexo,
  'desconhecido'                                   AS faixa_etaria_mae,
  sex_newborn                                      AS sexo_bebe
FROM s
