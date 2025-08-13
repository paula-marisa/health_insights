

with s as (
  select * from HEALTH_INSIGHTS.RAW_STG_raw_stg.stg_births
)
select
  s.*,
  case when try_to_number(birth_weight_g) >= 2500 then 0 else 1 end as is_low_weight,
  case
    when gestational_weeks is not null then iff(gestational_weeks < 37, 1, 0)
    when gestation_code in ('1','2','3','4') then 1
    else 0
  end as is_premature,
  case when delivery_type = '2' then 1 else 0 end as is_cesarean
from s