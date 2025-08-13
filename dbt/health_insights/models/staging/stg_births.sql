{{ config(materialized='view') }}

with src as (
  select * from {{ source('raw_stg','sinasc_raw') }}
),
-- gera um número de linha estável para cada registo (ordenação determinística)
k as (
  select
    s.*,
    row_number() over (
      order by
        to_varchar(DTNASC), to_varchar(HORANASC), to_varchar(CODESTAB),
        to_varchar(CODMUNNASC), to_varchar(SEXO), cast(PESO as number),
        to_varchar(DTRECEBIM), to_varchar(DTCADASTRO), to_varchar(NUMEROLOTE),
        to_varchar(CONTADOR)
    ) as rn
  from src s
),
norm as (
  select
    -- chave única e estável baseada no rn + alguns campos
    md5(
      coalesce(to_varchar(CONTADOR),'') || '-' ||
      coalesce(to_varchar(CODMUNNASC),'') || '-' ||
      lpad(to_varchar(rn), 10, '0')
    )                                         as sk_birth,

    -- data de nascimento (aceita DATE, 'YYYY-MM-DD' ou 'YYYYMMDD')
    case
      when try_to_date(to_varchar(DTNASC)) is not null then try_to_date(to_varchar(DTNASC))
      else
        case
          when regexp_like(regexp_replace(to_varchar(DTNASC),'[^0-9]',''), '^[0-9]{8}$') then
            -- calcular candidatos
            coalesce(
              iff(
                try_to_date(regexp_replace(to_varchar(DTNASC),'[^0-9]',''), 'YYYYMMDD') is not null
                and year(try_to_date(regexp_replace(to_varchar(DTNASC),'[^0-9]',''), 'YYYYMMDD')) between 1990 and 2035,
                try_to_date(regexp_replace(to_varchar(DTNASC),'[^0-9]',''), 'YYYYMMDD'),
                null
              ),
              iff(
                try_to_date(regexp_replace(to_varchar(DTNASC),'[^0-9]',''), 'DDMMYYYY') is not null
                and year(try_to_date(regexp_replace(to_varchar(DTNASC),'[^0-9]',''), 'DDMMYYYY')) between 1990 and 2035,
                try_to_date(regexp_replace(to_varchar(DTNASC),'[^0-9]',''), 'DDMMYYYY'),
                null
              )
            )
          when regexp_like(regexp_replace(to_varchar(DTNASC),'[^0-9]',''), '^[0-9]{7}$') then
            try_to_date(lpad(regexp_replace(to_varchar(DTNASC),'[^0-9]',''), 8, '0'), 'DDMMYYYY')
          else
            try_to_date(to_varchar(DTNASC))
        end
    end                                        as birth_date,

    -- 1=M, 2=F, outros=U
    case to_varchar(SEXO)
      when '1' then 'M'
      when '2' then 'F'
      else 'U'
    end                                        as sex_newborn,

    cast(PESO as number)                       as birth_weight_g,
    to_varchar(GESTACAO)                       as gestation_code,
    cast(SEMAGESTAC as number)                 as gestational_weeks,
    to_varchar(PARTO)                          as delivery_type,
    to_varchar(CODMUNNASC)                     as municipality_code
  from k
)
select
  sk_birth, birth_date, sex_newborn, birth_weight_g,
  gestation_code, gestational_weeks, delivery_type, municipality_code,
  to_char(birth_date, 'YYYY-MM') as ym
from norm
where birth_date is not null
