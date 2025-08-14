
    
    

select
    municipality_code as unique_field,
    count(*) as n_records

from HEALTH_INSIGHTS.marts.dim_localidade
where municipality_code is not null
group by municipality_code
having count(*) > 1


