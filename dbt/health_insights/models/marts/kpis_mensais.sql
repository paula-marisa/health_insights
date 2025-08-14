{{ config(materialized="view") }}

SELECT
    t.ano,
    t.mes,
    l.uf_sigla,
    COUNT(*) AS total_nasc,
    COUNT_IF(gestacao_semanas < 37) / COUNT(*)::float AS taxa_prematuridade,
    COUNT_IF(tipo_parto = 'cesariana') / COUNT(*)::float AS taxa_cesarianas,
    COUNT_IF(peso < 2500) / COUNT(*)::float AS taxa_baixo_peso
FROM {{ ref('fato_nascimentos') }} f
JOIN {{ ref('dim_tempo') }} t ON f.fk_sk_tempo = t.sk_tempo
JOIN {{ ref('dim_localidade') }} l ON f.fk_sk_localidade = l.sk_localidade
GROUP BY t.ano, t.mes, l.uf_sigla
