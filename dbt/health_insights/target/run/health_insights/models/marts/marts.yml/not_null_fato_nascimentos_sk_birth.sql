
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select sk_birth
from `health_insights`.`marts`.`fato_nascimentos`
where sk_birth is null



  
  
      
    ) dbt_internal_test