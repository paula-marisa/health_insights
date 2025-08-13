
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    sk_faixa_etaria_sexo as unique_field,
    count(*) as n_records

from HEALTH_INSIGHTS.RAW_STG_marts.dim_faixa_etaria_sexo
where sk_faixa_etaria_sexo is not null
group by sk_faixa_etaria_sexo
having count(*) > 1



  
  
      
    ) dbt_internal_test