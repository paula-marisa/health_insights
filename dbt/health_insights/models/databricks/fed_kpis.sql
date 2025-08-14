{{ config(materialized='view', schema='silver') }}

with f as (
  select * from {{ ref('fed_fato') }}
)
select
  count(*) as n_registos,

  -- % prematuros (<37)
  {% if target.type in ['databricks','spark'] %}
    cast(avg(case when try_cast(gestational_weeks as double) < 37 then 1 else 0 end) * 100 as double) as pct_prematuros,
  {% else %}
    cast(avg(case when try_to_number(gestational_weeks) < 37 then 1 else 0 end) * 100 as float) as pct_prematuros,
  {% endif %}

  -- % cesarianas (labels + código 1)
  {% if target.type in ['databricks','spark'] %}
    cast(
      avg(
        case
          when upper(coalesce(delivery_type,'')) rlike '^(CESAREA|CESÁREA|CESAREAN|CESARIAN|C)$' then 1
          when try_cast(coalesce(delivery_type,'') as int) in (1) then 1
          else 0
        end
      ) * 100 as double
    ) as pct_cesarianas,
  {% else %}
    cast(
      avg(
        case
          when regexp_like(upper(coalesce(delivery_type,'')),'^(CESAREA|CESÁREA|CESAREAN|CESARIAN|C)$') then 1
          when try_to_number(coalesce(delivery_type,'')) in (1) then 1
          else 0
        end
      ) * 100 as float
    ) as pct_cesarianas,
  {% endif %}

  avg(try_cast(birth_weight_g as double)) as peso_medio_g

from f
