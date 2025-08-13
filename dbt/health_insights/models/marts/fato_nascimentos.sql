{{ config(materialized='table') }}

select
  sk_birth,
  birth_date,
  ym,
  sex_newborn,
  birth_weight_g,
  is_low_weight,
  is_premature,
  is_cesarean,
  municipality_code
from {{ ref('int_births_enriched') }}
where birth_date between to_date('{{ var("analysis_start") }}') and to_date('{{ var("analysis_end") }}')
