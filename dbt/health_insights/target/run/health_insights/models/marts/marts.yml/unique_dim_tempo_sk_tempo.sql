
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    sk_tempo as unique_field,
    count(*) as n_records

from HEALTH_INSIGHTS.RAW_STG_marts.dim_tempo
where sk_tempo is not null
group by sk_tempo
having count(*) > 1



  
  
      
    ) dbt_internal_test