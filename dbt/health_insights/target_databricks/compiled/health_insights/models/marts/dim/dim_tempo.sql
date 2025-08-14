

with d as (
  select distinct 
  
    cast(birth_date as date)
  
 as data
  from `health_insights`.`silver`.`int_births_enriched`
  where birth_date is not null
)
select
  md5(
  
    date_format(data, 'yyyy-MM-dd')
  
)      as sk_tempo,
  data                                as data_dia,
  year(data)                          as ano,
  month(data)                         as mes,
  
  
    date_format(data, 'yyyy-MM')
  
             as ym,
  quarter(data)                       as trimestre,
  
  
    -- dayofweek: Dom=1 .. Sab=7 → converter para ISO (Seg=1 .. Dom=7)
    (((dayofweek(data) + 5) % 7) + 1)
  
             as dia_semana_iso,
  case when 
  
    -- dayofweek: Dom=1 .. Sab=7 → converter para ISO (Seg=1 .. Dom=7)
    (((dayofweek(data) + 5) % 7) + 1)
  
 in (6,7) then 1 else 0 end as is_fim_semana
from d