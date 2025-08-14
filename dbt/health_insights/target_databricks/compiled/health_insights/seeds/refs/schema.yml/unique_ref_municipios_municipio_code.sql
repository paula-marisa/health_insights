
    
    

select
    municipio_code as unique_field,
    count(*) as n_records

from `health_insights`.`raw_stg_marts`.`ref_municipios`
where municipio_code is not null
group by municipio_code
having count(*) > 1


