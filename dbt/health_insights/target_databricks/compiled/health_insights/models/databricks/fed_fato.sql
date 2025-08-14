

with f as (
  select * from `health_insights`.`marts`.`fato_nascimentos`
),
t as (
  select sk_tempo, data_dia, ano, mes from `health_insights`.`marts`.`dim_tempo`
)
select
  -- colunas necessárias da fato
  f.sk_birth,
  f.fk_sk_tempo,
  f.fk_sk_localidade,
  f.birth_weight_g,
  f.gestational_weeks,
  f.delivery_type,

  -- birth_date (compatível)
  
    t.data_dia as birth_date,
  

  -- ym canónico yyyy-MM
  
    lpad(cast(t.ano as string), 4, '0') || '-' || lpad(cast(t.mes as string), 2, '0') as ym
  
from f
left join t
  on f.fk_sk_tempo = t.sk_tempo