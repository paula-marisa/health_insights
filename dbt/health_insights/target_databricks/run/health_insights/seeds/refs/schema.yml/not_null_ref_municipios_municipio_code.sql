
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select municipio_code
from `health_insights`.`raw_stg_marts`.`ref_municipios`
where municipio_code is null



  
  
      
    ) dbt_internal_test