
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select uf_sigla
from HEALTH_INSIGHTS.RAW_STG.ref_uf
where uf_sigla is null



  
  
      
    ) dbt_internal_test