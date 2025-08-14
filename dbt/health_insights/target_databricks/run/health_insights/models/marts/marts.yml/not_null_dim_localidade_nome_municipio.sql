
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select nome_municipio
from `health_insights`.`marts`.`dim_localidade`
where nome_municipio is null



  
  
      
    ) dbt_internal_test