
    
    

with child as (
    select fk_sk_tempo as from_field
    from HEALTH_INSIGHTS.RAW_STG_marts.fato_nascimentos
    where fk_sk_tempo is not null
),

parent as (
    select sk_tempo as to_field
    from HEALTH_INSIGHTS.RAW_STG_marts.dim_tempo
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


