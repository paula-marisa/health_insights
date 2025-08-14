
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    sk_recem_nascido as unique_field,
    count(*) as n_records

from `health_insights`.`marts`.`dim_recem_nascido`
where sk_recem_nascido is not null
group by sk_recem_nascido
having count(*) > 1



  
  
      
    ) dbt_internal_test