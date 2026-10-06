{{ config(alias="silver_system_role") }}

-- Silver Mapas — system_role: papéis do sistema e suas permissões.
-- Origem: mapas.bronze_system_role, cópia fiel em que tudo chega como texto.
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
            {{ bronze_texto("slug") }} as slug,
            {{ bronze_texto("name") }} as name,
            {{ bronze_booleano("subsite_context") }} as subsite_context,
            {{ mapas_json("permissions") }} as permissions,
            {{ bronze_timestamp("create_timestamp") }} as create_timestamp,
            {{ bronze_timestamp("update_timestamp") }} as update_timestamp,
            {{ bronze_inteiro("status", "smallint") }} as status,
            _fatia
        from {{ source("mapas", "bronze_system_role") }}
    ),

    deduplicado as (
        select *, row_number() over (partition by id order by _fatia desc) as ordem
        from tipado
        where id is not null
    )

select
    id,
    slug,
    name,
    subsite_context,
    permissions,
    create_timestamp,
    update_timestamp,
    status
from deduplicado
where ordem = 1
