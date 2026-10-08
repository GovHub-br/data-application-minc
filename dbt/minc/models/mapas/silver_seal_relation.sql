-- Silver Mapas — seal_relation: selos concedidos a objetos.
-- Origem: mapas.bronze_seal_relation, cópia fiel em que tudo chega como texto.
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
            {{ bronze_inteiro("seal_id") }} as seal_id,
            {{ bronze_inteiro("object_id") }} as object_id,
            {{ bronze_timestamp("create_timestamp") }} as create_timestamp,
            {{ bronze_inteiro("status", "smallint") }} as status,
            {{ bronze_texto("object_type") }} as object_type,
            {{ bronze_inteiro("agent_id") }} as agent_id,
            {{ bronze_inteiro("owner_id") }} as owner_id,
            {{ bronze_data("validate_date") }} as validate_date,
            {{ bronze_booleano("renovation_request") }} as renovation_request,
            _fatia
        from {{ source("mapas", "bronze_seal_relation") }}
    ),

    deduplicado as (
        select *, row_number() over (partition by id order by _fatia desc) as ordem
        from tipado
        where id is not null
    )

select
    id,
    seal_id,
    object_id,
    create_timestamp,
    status,
    object_type,
    agent_id,
    owner_id,
    validate_date,
    renovation_request
from deduplicado
where ordem = 1
