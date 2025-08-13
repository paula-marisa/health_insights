
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select sk_mae
from HEALTH_INSIGHTS.RAW_STG_marts.dim_mae
where sk_mae is null



  
  
      
    ) dbt_internal_test