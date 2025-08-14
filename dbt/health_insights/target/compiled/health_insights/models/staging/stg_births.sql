






with src as (
  select * from HEALTH_INSIGHTS.RAW_STG.sinasc_raw
),

k as (
  
  
  
    
      
        
      
    
  
    
      
        
      
    
  
    
      
        
      
    
  
    
      
        
      
    
  
    
      
        
      
    
  
    
      
        
      
    
  
    
      
        
      
    
  
    
      
        
      
    
  
    
      
        
      
    
  
    
      
        
      
    
  

  select
    s.*,
    row_number() over (
      order by 
  
    cast(DTNASC as varchar)
  
, 
  
    cast(HORANASC as varchar)
  
, 
  
    cast(CODESTAB as varchar)
  
, 
  
    cast(CODMUNNASC as varchar)
  
, 
  
    cast(SEXO as varchar)
  
, 
  -- int funciona em Snowflake e Spark
  cast(PESO as int)
, 
  
    cast(DTRECEBIM as varchar)
  
, 
  
    cast(DTCADASTRO as varchar)
  
, 
  
    cast(NUMEROLOTE as varchar)
  
, 
  
    cast(CONTADOR as varchar)
  

    ) as rn
  from src s
),

norm as (
  select
    -- SK: usar macro portátil
    md5(cast(coalesce(cast(coalesce(
  
    cast(CONTADOR as varchar)
  
, '') as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(coalesce(
  
    cast(CODMUNNASC as varchar)
  
, '') as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(lpad(cast(rn as TEXT), 10, '0') as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as sk_birth,

    -- data de nascimento: várias formas
    case
      when 
  
    regexp_like(
  
    cast(DTNASC as varchar)
  
, '^[0-9]{4}-[0-9]{2}-[0-9]{2}$')
  
 then
        
  
    
      try_to_date(
  
    cast(DTNASC as varchar)
  
)
    
  

      when 
  
    regexp_like(regexp_replace(
  
    cast(DTNASC as varchar)
  
,'[^0-9]',''), '^[0-9]{8}$')
  
 then
        coalesce(
          
  
    
      try_to_date(regexp_replace(
  
    cast(DTNASC as varchar)
  
,'[^0-9]',''), 'DDMMYYYY')
    
  
,
          
  
    
      try_to_date(regexp_replace(
  
    cast(DTNASC as varchar)
  
,'[^0-9]',''), 'YYYYMMDD')
    
  

        )
      when 
  
    regexp_like(regexp_replace(
  
    cast(DTNASC as varchar)
  
,'[^0-9]',''), '^[0-9]{7}$')
  
 then
        
  
    
      try_to_date(lpad(regexp_replace(
  
    cast(DTNASC as varchar)
  
,'[^0-9]',''), 8, '0'), 'DDMMYYYY')
    
  

      else null
    end as birth_date,

    case 
  
    cast(SEXO as varchar)
  

      when '1' then 'M'
      when '2' then 'F'
      else 'U'
    end as sex_newborn,

    -- nulos portáveis
             
  -- int funciona em Snowflake e Spark
  cast(PESO as int)

               as birth_weight_g,

         
  
    cast(GESTACAO as varchar)
  

            as gestation_code,

       
  -- int funciona em Snowflake e Spark
  cast(SEMAGESTAC as int)

               as gestational_weeks,

            
  
    cast(PARTO as varchar)
  

            as delivery_type,

       
  
    cast(CODMUNNASC as varchar)
  

            as municipality_code
  from k
)

select
  sk_birth, birth_date, sex_newborn, birth_weight_g,
  gestation_code, gestational_weeks, delivery_type, municipality_code,
  
  
    to_char(birth_date, 'YYYY-MM')
  
 as ym
from norm
where birth_date is not null