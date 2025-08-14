
    
    

with all_values as (

    select
        sex_newborn as value_field,
        count(*) as n_records

    from `health_insights`.`marts`.`fato_nascimentos`
    group by sex_newborn

)

select *
from all_values
where value_field not in (
    'M','F','U'
)


