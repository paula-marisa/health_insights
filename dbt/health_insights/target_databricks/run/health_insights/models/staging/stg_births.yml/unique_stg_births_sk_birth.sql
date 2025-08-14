
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    sk_birth as unique_field,
    count(*) as n_records

from `health_insights`.`raw_stg`.`stg_births`
where sk_birth is not null
group by sk_birth
having count(*) > 1



  
  
      
    ) dbt_internal_test