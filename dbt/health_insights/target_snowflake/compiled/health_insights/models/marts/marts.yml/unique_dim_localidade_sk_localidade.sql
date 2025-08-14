
    
    

select
    sk_localidade as unique_field,
    count(*) as n_records

from HEALTH_INSIGHTS.marts.dim_localidade
where sk_localidade is not null
group by sk_localidade
having count(*) > 1


