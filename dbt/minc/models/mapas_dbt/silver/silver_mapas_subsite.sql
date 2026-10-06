{{ config(alias="silver_subsite") }}

-- Silver Mapas — subsite: subsites (instâncias) da plataforma.
-- Origem: mapas.bronze_subsite, cópia fiel em que tudo chega como texto.
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
            {{ bronze_texto("name") }} as name,
            {{ bronze_timestamp("create_timestamp") }} as create_timestamp,
            {{ bronze_inteiro("status", "smallint") }} as status,
            {{ bronze_inteiro("agent_id") }} as agent_id,
            {{ bronze_texto("url") }} as url,
            {{ bronze_texto("namespace") }} as namespace,
            {{ bronze_texto("alias_url") }} as alias_url,
            {{ bronze_texto("verified_seals") }} as verified_seals,
            _fatia
        from {{ source("mapas", "bronze_subsite") }}
    ),

    deduplicado as (
        select *, row_number() over (partition by id order by _fatia desc) as ordem
        from tipado
        where id is not null
    )

select
    id,
    name,
    create_timestamp,
    status,
    agent_id,
    url,
    namespace,
    alias_url,
    verified_seals
from deduplicado
where ordem = 1
