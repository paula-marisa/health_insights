
    
  create or replace table `health_insights`.`raw_stg`.`check_conn`
  
  (
    
      user_name string,
    
      catalog_name string,
    
      schema_name string,
    
      ts timestamp
    
    
  )

  using delta
  
  
  
  
  
  
  

  