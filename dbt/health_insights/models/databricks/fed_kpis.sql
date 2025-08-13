{{ config(materialized='view') }}

with f as (select * from {{ ref('fato_nascimentos') }})
select
  date_trunc('month', f.birth_date) as mes,
  count(*)                          as nacimentos,
  avg(case when f.is_cesarean then 1 else 0 end) as perc_cesarea,
  avg(case when f.is_premature then 1 else 0 end) as perc_prematuros
from f
group by 1
order by 1;
