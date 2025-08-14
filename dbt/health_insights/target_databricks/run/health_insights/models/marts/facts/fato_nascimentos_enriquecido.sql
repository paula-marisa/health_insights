-- back compat for old kwarg name
  
  
  
  
  
  
      
          
          
      
  

    merge
    into
        `health_insights`.`marts`.`fato_nascimentos_enriquecido` as DBT_INTERNAL_DEST
    using
        `fato_nascimentos_enriquecido__dbt_tmp` as DBT_INTERNAL_SOURCE
    on
        
              DBT_INTERNAL_SOURCE.sk_birth <=> DBT_INTERNAL_DEST.sk_birth
          
    when matched
        then update set
            `sk_birth` = DBT_INTERNAL_SOURCE.`sk_birth`, `municipality_code` = DBT_INTERNAL_SOURCE.`municipality_code`, `birth_date` = DBT_INTERNAL_SOURCE.`birth_date`, `year_month_date` = DBT_INTERNAL_SOURCE.`year_month_date`, `ym` = DBT_INTERNAL_SOURCE.`ym`, `sex_newborn` = DBT_INTERNAL_SOURCE.`sex_newborn`, `birth_weight_g` = DBT_INTERNAL_SOURCE.`birth_weight_g`, `gestational_weeks` = DBT_INTERNAL_SOURCE.`gestational_weeks`, `delivery_type` = DBT_INTERNAL_SOURCE.`delivery_type`, `state_code` = DBT_INTERNAL_SOURCE.`state_code`, `sk_localidade` = DBT_INTERNAL_SOURCE.`sk_localidade`, `nome_municipio` = DBT_INTERNAL_SOURCE.`nome_municipio`, `state_name` = DBT_INTERNAL_SOURCE.`state_name`
    when not matched
        then insert
            (`sk_birth`, `municipality_code`, `birth_date`, `year_month_date`, `ym`, `sex_newborn`, `birth_weight_g`, `gestational_weeks`, `delivery_type`, `state_code`, `sk_localidade`, `nome_municipio`, `state_name`) VALUES (DBT_INTERNAL_SOURCE.`sk_birth`, DBT_INTERNAL_SOURCE.`municipality_code`, DBT_INTERNAL_SOURCE.`birth_date`, DBT_INTERNAL_SOURCE.`year_month_date`, DBT_INTERNAL_SOURCE.`ym`, DBT_INTERNAL_SOURCE.`sex_newborn`, DBT_INTERNAL_SOURCE.`birth_weight_g`, DBT_INTERNAL_SOURCE.`gestational_weeks`, DBT_INTERNAL_SOURCE.`delivery_type`, DBT_INTERNAL_SOURCE.`state_code`, DBT_INTERNAL_SOURCE.`sk_localidade`, DBT_INTERNAL_SOURCE.`nome_municipio`, DBT_INTERNAL_SOURCE.`state_name`)

