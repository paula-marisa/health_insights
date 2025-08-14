
  
    

create or replace transient table HEALTH_INSIGHTS.marts.dim_tempo
    
    
    
    as (

with d as (
  select distinct 
  
    cast(birth_date as date)
  
 as data
  from HEALTH_INSIGHTS.silver.int_births_enriched
  where birth_date is not null
)
select
  md5(
  
    to_char(data, 'YYYY-MM-DD')
  
)      as sk_tempo,
  data                                as data_dia,
  year(data)                          as ano,
  month(data)                         as mes,
  
  
    to_char(data, 'YYYY-MM')
  
             as ym,
  quarter(data)                       as trimestre,
  
  
    dayofweekiso(data)
  
             as dia_semana_iso,
  case when 
  
    dayofweekiso(data)
  
 in (6,7) then 1 else 0 end as is_fim_semana
from d
    )
;


  