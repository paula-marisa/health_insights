
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    sk_mae as unique_field,
    count(*) as n_records

from `health_insights`.`marts`.`dim_mae`
where sk_mae is not null
group by sk_mae
having count(*) > 1



  
  
      
    ) dbt_internal_test