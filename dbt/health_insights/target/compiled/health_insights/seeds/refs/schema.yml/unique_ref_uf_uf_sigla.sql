
    
    

select
    uf_sigla as unique_field,
    count(*) as n_records

from HEALTH_INSIGHTS.RAW_STG.ref_uf
where uf_sigla is not null
group by uf_sigla
having count(*) > 1


