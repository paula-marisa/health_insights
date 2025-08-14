
    
    

select
    uf_sigla as unique_field,
    count(*) as n_records

from `health_insights`.`raw_stg_marts`.`ref_uf`
where uf_sigla is not null
group by uf_sigla
having count(*) > 1


