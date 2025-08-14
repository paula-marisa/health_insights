
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select state_code
from `health_insights`.`marts`.`dim_localidade`
where state_code is null



  
  
      
    ) dbt_internal_test