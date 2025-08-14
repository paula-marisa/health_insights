
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        categoria_peso as value_field,
        count(*) as n_records

    from `health_insights`.`marts`.`dim_recem_nascido`
    group by categoria_peso

)

select *
from all_values
where value_field not in (
    'baixo_peso','adequado','macrossomico','desconhecido'
)



  
  
      
    ) dbt_internal_test