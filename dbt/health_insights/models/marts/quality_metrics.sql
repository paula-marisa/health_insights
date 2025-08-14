{{ config(materialized="view") }}

SELECT
    CURRENT_TIMESTAMP AS verificado_em,
    COUNT(*) AS total_registos,
    COUNT_IF(birth_date IS NULL) AS nulos_birth_date,
    COUNT_IF(fk_sk_localidade IS NULL) AS nulos_localidade,
    COUNT(DISTINCT sk_nascimento) AS ids_unicos,
    (COUNT(*) - COUNT(DISTINCT sk_nascimento)) AS duplicados
FROM {{ ref('fato_nascimentos') }}
