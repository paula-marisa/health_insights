
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select data_dia
from HEALTH_INSIGHTS.RAW_STG_marts.dim_tempo
where data_dia is null



  
  
      
    ) dbt_internal_test