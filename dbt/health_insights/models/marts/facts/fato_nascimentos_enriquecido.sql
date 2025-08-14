-- models/marts/fato_nascimentos_enriquecido.sql
{{ config(materialized='incremental', unique_key='sk_birth', on_schema_change='sync_all_columns') }}

with f as (
  select * from {{ ref('int_births_enriched') }}
)
select
  f.*,
  d.sk_localidade,
  d.nome_municipio,
  d.state_name
from f
left join {{ ref('dim_localidade') }} d
  on d.municipality_code = f.municipality_code