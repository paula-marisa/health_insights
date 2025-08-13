
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select sk_recem_nascido
from HEALTH_INSIGHTS.RAW_STG_marts.dim_recem_nascido
where sk_recem_nascido is null



  
  
      
    ) dbt_internal_test