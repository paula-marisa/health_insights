
    
    

with all_values as (

    select
        categoria_peso as value_field,
        count(*) as n_records

    from HEALTH_INSIGHTS.RAW_STG_marts.dim_recem_nascido
    group by categoria_peso

)

select *
from all_values
where value_field not in (
    'baixo_peso','adequado','macrossomico','desconhecido'
)


