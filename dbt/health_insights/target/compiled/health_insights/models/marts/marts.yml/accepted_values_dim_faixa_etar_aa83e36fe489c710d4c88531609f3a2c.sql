
    
    

with all_values as (

    select
        faixa_etaria_mae as value_field,
        count(*) as n_records

    from HEALTH_INSIGHTS.marts.dim_faixa_etaria_sexo
    group by faixa_etaria_mae

)

select *
from all_values
where value_field not in (
    'desconhecido'
)


