{{ config(alias="silver_event_occurrence") }}

-- Silver Mapas — event_occurrence: ocorrências (data, hora e local) dos eventos.
-- Origem: mapas.bronze_event_occurrence, cópia fiel em que tudo chega como texto.
-- Esta camada só limpa, tipa e tira duplicata; não cruza tabelas nem fontes.
--
-- Grão: 1 linha por `id`.
--
-- Tipagem pelos tipos que o banco do Mapas declara. Texto vazio e só de espaços viram
-- NULL;
-- valor que não casa com o tipo vira NULL; ausente fica nulo, nunca vira falso.
with
    tipado as (
        select
            {{ bronze_inteiro("id") }} as id,
            {{ bronze_inteiro("space_id") }} as space_id,
            {{ bronze_inteiro("event_id") }} as event_id,
            {{ bronze_texto("rule") }} as rule,
            {{ bronze_data("starts_on") }} as starts_on,
            {{ bronze_data("ends_on") }} as ends_on,
            {{ bronze_timestamp("starts_at") }} as starts_at,
            {{ bronze_timestamp("ends_at") }} as ends_at,
            {{ bronze_texto("frequency") }} as frequency,
            {{ bronze_inteiro("separation") }} as separation,
            {{ bronze_inteiro("count") }} as count,
            {{ bronze_data("until") }} as until,
            {{ bronze_texto("timezone_name") }} as timezone_name,
            {{ bronze_inteiro("status") }} as status,
            {{ bronze_texto("description") }} as description,
            {{ bronze_texto("price") }} as price,
            {{ bronze_texto("priceinfo") }} as priceinfo,
            _fatia
        from {{ source("mapas", "bronze_event_occurrence") }}
    ),

    deduplicado as (
        select *, row_number() over (partition by id order by _fatia desc) as ordem
        from tipado
        where id is not null
    )

select
    id,
    space_id,
    event_id,
    rule,
    starts_on,
    ends_on,
    starts_at,
    ends_at,
    frequency,
    separation,
    count,
    until,
    timezone_name,
    status,
    description,
    price,
    priceinfo
from deduplicado
where ordem = 1
