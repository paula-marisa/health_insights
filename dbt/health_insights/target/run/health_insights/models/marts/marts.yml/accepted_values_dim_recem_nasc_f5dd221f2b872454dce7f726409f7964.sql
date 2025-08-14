
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        categoria_gestacao as value_field,
        count(*) as n_records

    from HEALTH_INSIGHTS.marts.dim_recem_nascido
    group by categoria_gestacao

)

select *
from all_values
where value_field not in (
    'pre_termo','termo','pos_termo','desconhecido'
)



  
  
      
    ) dbt_internal_test