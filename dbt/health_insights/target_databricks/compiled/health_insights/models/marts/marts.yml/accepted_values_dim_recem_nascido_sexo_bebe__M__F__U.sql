
    
    

with all_values as (

    select
        sexo_bebe as value_field,
        count(*) as n_records

    from `health_insights`.`marts`.`dim_recem_nascido`
    group by sexo_bebe

)

select *
from all_values
where value_field not in (
    'M','F','U'
)


