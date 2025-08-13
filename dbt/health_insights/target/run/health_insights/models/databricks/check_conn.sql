
  
    

create or replace transient table HEALTH_INSIGHTS.RAW_STG.check_conn
    
    
    
    as (
select current_user() as user_name, current_catalog() as catalog_name,
       current_schema() as schema_name, current_timestamp() as ts
    )
;


  