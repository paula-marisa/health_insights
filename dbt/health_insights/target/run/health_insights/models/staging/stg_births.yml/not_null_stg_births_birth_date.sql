
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select birth_date
from HEALTH_INSIGHTS.RAW_STG_raw_stg.stg_births
where birth_date is null



  
  
      
    ) dbt_internal_test