
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select faixa_etaria_mae
from HEALTH_INSIGHTS.RAW_STG_marts.dim_faixa_etaria_sexo
where faixa_etaria_mae is null



  
  
      
    ) dbt_internal_test