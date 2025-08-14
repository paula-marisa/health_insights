

with f as (
  select * from HEALTH_INSIGHTS.RAW_STG_marts.fato_nascimentos
)
select
  -- mês civil (funciona em Snowflake e Spark/Databricks)
  date_trunc('month', f.birth_date) as mes,

  -- contagem
  count(*) as nascimentos,

  -- percentagem de cesarianas (0/1), *100 e CAST para float adequado
  CAST(avg(case when f.is_cesarean then 1 else 0 end) * 100 AS FLOAT) as perc_cesarea,

  -- percentagem de prematuros
  CAST(avg(case when f.is_premature then 1 else 0 end) * 100 AS FLOAT) as perc_prematuros

from f
group by 1
order by 1