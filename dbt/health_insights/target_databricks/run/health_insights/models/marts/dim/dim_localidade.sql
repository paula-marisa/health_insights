
    
  create or replace table `health_insights`.`marts`.`dim_localidade`
  
  (
    
      sk_localidade string,
    
      municipality_code bigint,
    
      state_code string,
    
      state_name string,
    
      nome_municipio string,
    
      regiao string
    
    
  )

  using delta
  
  
  
  
  
  
  

  