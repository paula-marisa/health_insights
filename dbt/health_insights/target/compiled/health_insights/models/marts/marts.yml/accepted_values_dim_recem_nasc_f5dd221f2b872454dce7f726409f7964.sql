
    
    

with all_values as (

    select
        categoria_gestacao as value_field,
        count(*) as n_records

    from HEALTH_INSIGHTS.RAW_STG_marts.dim_recem_nascido
    group by categoria_gestacao

)

select *
from all_values
where value_field not in (
    'pre_termo','termo','pos_termo','desconhecido'
)


