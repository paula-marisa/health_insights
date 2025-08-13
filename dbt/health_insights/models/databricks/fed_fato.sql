{{ config(materialized='view') }}

with f as (
  select * from {{ ref('fato_nascimentos') }}
)
select * from f;
