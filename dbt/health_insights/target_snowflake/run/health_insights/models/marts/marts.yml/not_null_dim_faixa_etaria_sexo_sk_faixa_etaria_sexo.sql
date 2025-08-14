
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select sk_faixa_etaria_sexo
from HEALTH_INSIGHTS.marts.dim_faixa_etaria_sexo
where sk_faixa_etaria_sexo is null



  
  
      
    ) dbt_internal_test