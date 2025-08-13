
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select sk_localidade
from HEALTH_INSIGHTS.RAW_STG_marts.dim_localidade
where sk_localidade is null



  
  
      
    ) dbt_internal_test