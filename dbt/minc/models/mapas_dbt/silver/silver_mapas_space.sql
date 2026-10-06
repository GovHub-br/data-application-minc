{{ config(alias="silver_space") }}

-- Silver Mapas — space: espaços culturais.
-- Origem: mapas.bronze_space, cópia fiel em que tudo chega como texto.
-- Esta camada só limpa, tipa e tira duplicata; não cruza tabelas nem fontes.
--
-- Grão: 1 linha por `id`.
--
-- Tipagem pelos tipos que o banco do Mapas declara. Texto vazio e só de espaços viram
-- NULL;
-- valor que não casa com o tipo vira NULL; ausente fica nulo, nunca vira falso.
--
-- `location` é um `point` do PostgreSQL ([x,y]) e sai como `longitude` e `latitude`;
-- ficam NULL o intervalo
-- impossível e o ponto (0,0), que o cadastro grava quando não há localização (macro
-- mapas_ponto).
-- `_geo_location` (geography) fica como texto EWKB, porque o DW não depende do PostGIS.
with
    tipado as (
        select
            {{ bronze_inteiro("id") }} as id,
            {{ bronze_inteiro("parent_id") }} as parent_id,
            {{ mapas_ponto("location", 1) }} as longitude,
            {{ mapas_ponto("location", 2) }} as latitude,
            {{ bronze_texto("_geo_location") }} as _geo_location,
            {{ bronze_texto("name") }} as name,
            {{ bronze_texto("short_description") }} as short_description,
            {{ bronze_texto("long_description") }} as long_description,
            {{ bronze_timestamp("create_timestamp") }} as create_timestamp,
            {{ bronze_inteiro("status", "smallint") }} as status,
            {{ bronze_inteiro("type", "smallint") }} as type,
            {{ bronze_inteiro("agent_id") }} as agent_id,
            {{ bronze_booleano("is_verified") }} as is_verified,
            {{ bronze_booleano("public") }} as public,
            {{ bronze_timestamp("update_timestamp") }} as update_timestamp,
            {{ bronze_inteiro("codigo") }} as codigo,
            {{ bronze_inteiro("subsite_id") }} as subsite_id,
            _fatia
        from {{ source("mapas", "bronze_space") }}
    ),

    deduplicado as (
        select *, row_number() over (partition by id order by _fatia desc) as ordem
        from tipado
        where id is not null
    )

select
    id,
    parent_id,
    longitude,
    latitude,
    _geo_location,
    name,
    short_description,
    long_description,
    create_timestamp,
    status,
    type,
    agent_id,
    is_verified,
    public,
    update_timestamp,
    codigo,
    subsite_id
from deduplicado
where ordem = 1
