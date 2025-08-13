
  create or replace   view HEALTH_INSIGHTS.RAW_STG.fed_fato
  
  
  
  
  as (
    
select *
from sf_hi.raw_stg_marts.fato_nascimentos
  );

