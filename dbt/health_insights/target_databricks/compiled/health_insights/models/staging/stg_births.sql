






with src as (
  select * from `health_insights`.`raw_stg`.`sinasc_raw`
),

k as (
  
  
  
    
      
        
      
    
  
    
  
    
  
    
      
        
      
    
  
    
      
        
      
    
  
    
      
        
      
    
  
    
  
    
  
    
  
    
  

  select
    s.*,
    row_number() over (
      order by 
  
    cast(DTNASC as string)
  
, 
  
    cast(CODMUNNASC as string)
  
, 
  
    cast(SEXO as string)
  
, 
  -- int funciona em Snowflake e Spark
  cast(PESO as int)

    ) as rn
  from src s
),

norm as (
  select
    -- usa CONTADOR/CODMUNNASC se existirem; caso contrário usa NULL para estabilizar o hash
    md5(
      coalesce( cast(null as string) , '') || '-' ||
      coalesce( 
  
    cast(CODMUNNASC as string)
  
 , '') || '-' ||
      lpad(
  
    cast(rn as string)
  
, 10, '0')
    ) as sk_birth,

    -- data de nascimento: várias formas
    case
      when 
  
    (
  
    cast(DTNASC as string)
  
 rlike '^[0-9]{4}-[0-9]{2}-[0-9]{2}$')
  
 then
        
  
    
      to_date(
  
    cast(DTNASC as string)
  
)
    
  

      when 
  
    (regexp_replace(
  
    cast(DTNASC as string)
  
,'[^0-9]','') rlike '^[0-9]{8}$')
  
 then
        coalesce(
          
  
    
      to_date(regexp_replace(
  
    cast(DTNASC as string)
  
,'[^0-9]',''), 'ddmmyyyy')
    
  
,
          
  
    
      to_date(regexp_replace(
  
    cast(DTNASC as string)
  
,'[^0-9]',''), 'yyyymmdd')
    
  

        )
      when 
  
    (regexp_replace(
  
    cast(DTNASC as string)
  
,'[^0-9]','') rlike '^[0-9]{7}$')
  
 then
        
  
    
      to_date(lpad(regexp_replace(
  
    cast(DTNASC as string)
  
,'[^0-9]',''), 8, '0'), 'ddmmyyyy')
    
  

      else null
    end as birth_date,

    case 
  
    cast(SEXO as string)
  

      when '1' then 'M'
      when '2' then 'F'
      else 'U'
    end as sex_newborn,

             
  -- int funciona em Snowflake e Spark
  cast(PESO as int)
        as birth_weight_g,
         
  
    cast(GESTACAO as string)
  
    as gestation_code,
       
  -- int funciona em Snowflake e Spark
  cast(SEMAGESTAC as int)
  as gestational_weeks,
     cast(null as string)  as delivery_type,
       
  
    cast(CODMUNNASC as string)
  
  as municipality_code
  from k
)

select
  sk_birth, birth_date, sex_newborn, birth_weight_g,
  gestation_code, gestational_weeks, delivery_type, municipality_code,
  
  
    date_format(birth_date, 'yyyy-MM')
  
 as ym
from norm
where birth_date is not null