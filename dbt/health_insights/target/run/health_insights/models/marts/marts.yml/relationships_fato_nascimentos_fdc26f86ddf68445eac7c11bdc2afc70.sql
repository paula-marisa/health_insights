
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with child as (
    select fk_sk_recem_nascido as from_field
    from HEALTH_INSIGHTS.RAW_STG_marts.fato_nascimentos
    where fk_sk_recem_nascido is not null
),

parent as (
    select sk_recem_nascido as to_field
    from HEALTH_INSIGHTS.RAW_STG_marts.dim_recem_nascido
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



  
  
      
    ) dbt_internal_test