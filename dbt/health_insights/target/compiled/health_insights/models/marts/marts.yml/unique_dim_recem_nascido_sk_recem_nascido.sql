
    
    

select
    sk_recem_nascido as unique_field,
    count(*) as n_records

from HEALTH_INSIGHTS.marts.dim_recem_nascido
where sk_recem_nascido is not null
group by sk_recem_nascido
having count(*) > 1


