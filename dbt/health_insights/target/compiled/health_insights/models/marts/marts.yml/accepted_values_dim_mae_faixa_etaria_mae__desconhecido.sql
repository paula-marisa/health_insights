
    
    

with all_values as (

    select
        faixa_etaria_mae as value_field,
        count(*) as n_records

    from `health_insights`.`marts`.`dim_mae`
    group by faixa_etaria_mae

)

select *
from all_values
where value_field not in (
    'desconhecido'
)


