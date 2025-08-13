{{ config(materialized='table') }}
select current_user() as user_name, current_catalog() as catalog_name,
       current_schema() as schema_name, current_timestamp() as ts
