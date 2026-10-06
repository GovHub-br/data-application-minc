{{ config(alias="silver_seal") }}

-- Silver Mapas — seal: selos.
-- Origem: mapas.bronze_seal, cópia fiel em que tudo chega como texto.
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
            {{ bronze_inteiro("agent_id") }} as agent_id,
            {{ bronze_texto("name") }} as name,
            {{ bronze_texto("short_description") }} as short_description,
            {{ bronze_texto("long_description") }} as long_description,
            {{ bronze_inteiro("valid_period", "smallint") }} as valid_period,
            {{ bronze_timestamp("create_timestamp") }} as create_timestamp,
            {{ bronze_inteiro("status", "smallint") }} as status,
            {{ bronze_texto("certificate_text") }} as certificate_text,
            {{ bronze_timestamp("update_timestamp") }} as update_timestamp,
            {{ bronze_inteiro("subsite_id") }} as subsite_id,
            {{ mapas_json("locked_fields") }} as locked_fields,
            _fatia
        from {{ source("mapas", "bronze_seal") }}
    ),

    deduplicado as (
        select *, row_number() over (partition by id order by _fatia desc) as ordem
        from tipado
        where id is not null
    )

select
    id,
    agent_id,
    name,
    short_description,
    long_description,
    valid_period,
    create_timestamp,
    status,
    certificate_text,
    update_timestamp,
    subsite_id,
    locked_fields
from deduplicado
where ordem = 1
