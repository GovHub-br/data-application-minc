{{ config(alias="silver_project") }}

-- Silver Mapas — project: projetos.
-- Origem: mapas.bronze_project, cópia fiel em que tudo chega como texto.
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
            {{ bronze_texto("short_description") }} as short_description,
            {{ bronze_texto("long_description") }} as long_description,
            {{ bronze_timestamp("create_timestamp") }} as create_timestamp,
            {{ bronze_inteiro("status", "smallint") }} as status,
            {{ bronze_inteiro("agent_id") }} as agent_id,
            {{ bronze_booleano("is_verified") }} as is_verified,
            {{ bronze_inteiro("type", "smallint") }} as type,
            {{ bronze_inteiro("parent_id") }} as parent_id,
            {{ bronze_timestamp("starts_on") }} as starts_on,
            {{ bronze_timestamp("ends_on") }} as ends_on,
            {{ bronze_timestamp("update_timestamp") }} as update_timestamp,
            {{ bronze_inteiro("subsite_id") }} as subsite_id,
            _fatia
        from {{ source("mapas", "bronze_project") }}
    ),

    deduplicado as (
        select *, row_number() over (partition by id order by _fatia desc) as ordem
        from tipado
        where id is not null
    )

select
    id,
    name,
    short_description,
    long_description,
    create_timestamp,
    status,
    agent_id,
    is_verified,
    type,
    parent_id,
    starts_on,
    ends_on,
    update_timestamp,
    subsite_id
from deduplicado
where ordem = 1
