{{ config(materialized='view') }}
select
  count(*)                        as n_nascidos,
  round(avg(birth_weight_g), 0)   as peso_medio_g,
  round(avg(is_premature)*100, 2) as pct_prematuros,
  round(avg(is_cesarean)*100, 2)  as pct_cesareas
from sf_hi.raw_stg_marts.fato_nascimentos
