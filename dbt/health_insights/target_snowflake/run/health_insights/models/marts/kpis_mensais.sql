
  create or replace   view HEALTH_INSIGHTS.marts.kpis_mensais
  
  
  
  
  as (
    

SELECT
    t.ano,
    t.mes,
    l.uf_sigla,
    COUNT(*) AS total_nasc,
    COUNT_IF(gestacao_semanas < 37) / COUNT(*)::float AS taxa_prematuridade,
    COUNT_IF(tipo_parto = 'cesariana') / COUNT(*)::float AS taxa_cesarianas,
    COUNT_IF(peso < 2500) / COUNT(*)::float AS taxa_baixo_peso
FROM HEALTH_INSIGHTS.marts.fato_nascimentos f
JOIN HEALTH_INSIGHTS.marts.dim_tempo t ON f.fk_sk_tempo = t.sk_tempo
JOIN HEALTH_INSIGHTS.marts.dim_localidade l ON f.fk_sk_localidade = l.sk_localidade
GROUP BY t.ano, t.mes, l.uf_sigla
  );

