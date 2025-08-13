
    
    

with all_values as (

    select
        sex_newborn as value_field,
        count(*) as n_records

    from HEALTH_INSIGHTS.RAW_STG_raw_stg.stg_births
    group by sex_newborn

)

select *
from all_values
where value_field not in (
    'M','F','U'
)


