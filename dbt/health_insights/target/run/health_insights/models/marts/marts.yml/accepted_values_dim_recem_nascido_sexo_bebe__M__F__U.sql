
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        sexo_bebe as value_field,
        count(*) as n_records

    from HEALTH_INSIGHTS.RAW_STG_marts.dim_recem_nascido
    group by sexo_bebe

)

select *
from all_values
where value_field not in (
    'M','F','U'
)



  
  
      
    ) dbt_internal_test