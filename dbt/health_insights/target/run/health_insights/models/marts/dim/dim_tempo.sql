
    
  create or replace table `health_insights`.`marts`.`dim_tempo`
  
  (
    
      sk_tempo string,
    
      data_dia date,
    
      ano int,
    
      mes int,
    
      ym string,
    
      trimestre int,
    
      dia_semana_iso int,
    
      is_fim_semana int
    
    
  )

  using delta
  
  
  
  
  
  
  

  