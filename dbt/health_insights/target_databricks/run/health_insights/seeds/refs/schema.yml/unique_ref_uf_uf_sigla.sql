
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    uf_sigla as unique_field,
    count(*) as n_records

from `health_insights`.`raw_stg_marts`.`ref_uf`
where uf_sigla is not null
group by uf_sigla
having count(*) > 1



  
  
      
    ) dbt_internal_test