-- back compat for old kwarg name
  
  
  
  
  
  
      
          
          
      
  

    merge
    into
        `health_insights`.`marts`.`fato_nascimentos` as DBT_INTERNAL_DEST
    using
        `fato_nascimentos__dbt_tmp` as DBT_INTERNAL_SOURCE
    on
        
              DBT_INTERNAL_SOURCE.sk_birth <=> DBT_INTERNAL_DEST.sk_birth
          
    when matched
        then update set
            `sk_birth` = DBT_INTERNAL_SOURCE.`sk_birth`, `fk_sk_tempo` = DBT_INTERNAL_SOURCE.`fk_sk_tempo`, `fk_sk_localidade` = DBT_INTERNAL_SOURCE.`fk_sk_localidade`, `fk_sk_recem_nascido` = DBT_INTERNAL_SOURCE.`fk_sk_recem_nascido`, `sex_newborn` = DBT_INTERNAL_SOURCE.`sex_newborn`, `birth_weight_g` = DBT_INTERNAL_SOURCE.`birth_weight_g`, `gestational_weeks` = DBT_INTERNAL_SOURCE.`gestational_weeks`, `delivery_type` = DBT_INTERNAL_SOURCE.`delivery_type`
    when not matched
        then insert
            (`sk_birth`, `fk_sk_tempo`, `fk_sk_localidade`, `fk_sk_recem_nascido`, `sex_newborn`, `birth_weight_g`, `gestational_weeks`, `delivery_type`) VALUES (DBT_INTERNAL_SOURCE.`sk_birth`, DBT_INTERNAL_SOURCE.`fk_sk_tempo`, DBT_INTERNAL_SOURCE.`fk_sk_localidade`, DBT_INTERNAL_SOURCE.`fk_sk_recem_nascido`, DBT_INTERNAL_SOURCE.`sex_newborn`, DBT_INTERNAL_SOURCE.`birth_weight_g`, DBT_INTERNAL_SOURCE.`gestational_weeks`, DBT_INTERNAL_SOURCE.`delivery_type`)

