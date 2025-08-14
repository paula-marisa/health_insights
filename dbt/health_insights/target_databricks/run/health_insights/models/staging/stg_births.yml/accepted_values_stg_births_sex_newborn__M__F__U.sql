
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        sex_newborn as value_field,
        count(*) as n_records

    from `health_insights`.`raw_stg`.`stg_births`
    group by sex_newborn

)

select *
from all_values
where value_field not in (
    'M','F','U'
)



  
  
      
    ) dbt_internal_test