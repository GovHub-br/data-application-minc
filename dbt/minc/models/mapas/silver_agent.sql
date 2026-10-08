-- Silver Mapas — agent: agentes culturais (pessoa física ou coletivo).
-- Origem: mapas.bronze_agent, cópia fiel em que tudo chega como texto.
-- Esta camada só limpa, tipa e tira duplicata; não cruza tabelas nem fontes.
--
-- Grão: 1 linha por agente (`id`).
--
-- Tipagem pelos tipos que o banco do Mapas declara. Texto vazio e só de
-- espaços viram NULL; valor que não casa com o tipo vira NULL; ausente fica
-- nulo, nunca vira falso.
--
-- `location` é um `point` do PostgreSQL ("(x,y)") e sai como duas colunas,
-- `longitude` e `latitude`. Ficam NULL: fora do intervalo possível e o ponto
-- (0,0), que é o que o cadastro grava quando não há localização (macro
-- mapas_ponto). Coordenada válida porém fora do Brasil permanece.
-- `_geo_location` é o `geography` em hexadecimal EWKB, mantido como texto, porque
-- o DW não depende do PostGIS para lê-lo.
--
-- Duplicata: a origem tem PK em `id`, então só aparece se a ingestão repetir
-- uma fatia. Fica a linha da fatia mais recente.
with
    tipado as (
        select
            {{ bronze_inteiro("id") }} as id,
            {{ bronze_inteiro("user_id") }} as user_id,
            {{ bronze_inteiro("type", "smallint") }} as type,
            {{ bronze_texto("name") }} as name,
            {{ mapas_ponto("location", 1) }} as longitude,
            {{ mapas_ponto("location", 2) }} as latitude,
            {{ bronze_texto("_geo_location") }} as _geo_location,
            {{ bronze_texto("short_description") }} as short_description,
            {{ bronze_texto("long_description") }} as long_description,
            {{ bronze_timestamp("create_timestamp") }} as create_timestamp,
            {{ bronze_inteiro("status", "smallint") }} as status,
            {{ bronze_booleano("is_verified") }} as is_verified,
            {{ bronze_inteiro("parent_id") }} as parent_id,
            {{ bronze_booleano("public_location") }} as public_location,
            {{ bronze_inteiro("id_responsavel_rac") }} as id_responsavel_rac,
            {{ bronze_inteiro("id_usuario_rac") }} as id_usuario_rac,
            {{ bronze_timestamp("update_timestamp") }} as update_timestamp,
            {{ bronze_inteiro("subsite_id") }} as subsite_id,
            _fatia
        from {{ source("mapas", "bronze_agent") }}
    ),

    deduplicado as (
        select *, row_number() over (partition by id order by _fatia desc) as ordem
        from tipado
        where id is not null
    )

select
    id,
    user_id,
    type,
    name,
    longitude,
    latitude,
    _geo_location,
    short_description,
    long_description,
    create_timestamp,
    status,
    is_verified,
    parent_id,
    public_location,
    id_responsavel_rac,
    id_usuario_rac,
    update_timestamp,
    subsite_id
from deduplicado
where ordem = 1
