
    
    

select
    sk_mae as unique_field,
    count(*) as n_records

from HEALTH_INSIGHTS.RAW_STG_marts.dim_mae
where sk_mae is not null
group by sk_mae
having count(*) > 1


