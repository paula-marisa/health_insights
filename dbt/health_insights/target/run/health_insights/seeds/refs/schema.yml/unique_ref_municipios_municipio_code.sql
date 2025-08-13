
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    municipio_code as unique_field,
    count(*) as n_records

from HEALTH_INSIGHTS.RAW_STG_RAW_STG.ref_municipios
where municipio_code is not null
group by municipio_code
having count(*) > 1



  
  
      
    ) dbt_internal_test