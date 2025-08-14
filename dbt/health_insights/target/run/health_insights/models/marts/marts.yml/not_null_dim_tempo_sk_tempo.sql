
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select sk_tempo
from `health_insights`.`marts`.`dim_tempo`
where sk_tempo is null



  
  
      
    ) dbt_internal_test