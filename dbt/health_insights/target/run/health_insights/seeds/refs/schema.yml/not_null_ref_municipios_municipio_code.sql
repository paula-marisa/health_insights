
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select municipio_code
from HEALTH_INSIGHTS.RAW_STG_RAW_STG.ref_municipios
where municipio_code is null



  
  
      
    ) dbt_internal_test