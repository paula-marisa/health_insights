
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select uf_sigla
from `health_insights`.`raw_stg_marts`.`ref_uf`
where uf_sigla is null



  
  
      
    ) dbt_internal_test