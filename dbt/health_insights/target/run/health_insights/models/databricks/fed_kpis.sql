
  
  create or replace view `health_insights`.`raw_stg`.`fed_kpis`
  
  as (
    

with f as (select * from `health_insights`.`raw_stg_marts`.`fato_nascimentos`)
select
  date_trunc('month', f.birth_date) as mes,
  count(*)                          as nacimentos,
  cast(avg(case when f.is_cesarean then 1 else 0 end) * 100 as double) as perc_cesarea,
  cast(avg(case when f.is_premature then 1 else 0 end) * 100 as double) as perc_prematuros
from f
group by 1
order by 1
  )
