
    
    

select
    sk_birth as unique_field,
    count(*) as n_records

from HEALTH_INSIGHTS.RAW_STG_raw_stg.stg_births
where sk_birth is not null
group by sk_birth
having count(*) > 1


