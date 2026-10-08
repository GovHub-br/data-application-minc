-- Silver Mapas — space_relation: relações dos espaços com agentes.
-- Origem: mapas.bronze_space_relation, cópia fiel em que tudo chega como texto.
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
            {{ bronze_inteiro("object_id") }} as object_id,
            {{ bronze_timestamp("create_timestamp") }} as create_timestamp,
            {{ bronze_inteiro("status", "smallint") }} as status,
            {{ bronze_texto("object_type") }} as object_type,
            _fatia
        from {{ source("mapas", "bronze_space_relation") }}
    ),

    deduplicado as (
        select *, row_number() over (partition by id order by _fatia desc) as ordem
        from tipado
        where id is not null
    )

select id, space_id, object_id, create_timestamp, status, object_type
from deduplicado
where ordem = 1
