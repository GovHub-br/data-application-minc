-- Silver Mapas — metalist: listas de links e vídeos associados aos objetos.
-- Origem: mapas.bronze_metalist, cópia fiel em que tudo chega como texto.
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
            {{ bronze_texto("object_type") }} as object_type,
            {{ bronze_inteiro("object_id") }} as object_id,
            {{ bronze_texto("grp") }} as grp,
            {{ bronze_texto("title") }} as title,
            {{ bronze_texto("description") }} as description,
            {{ bronze_texto("value") }} as value,
            {{ bronze_timestamp("create_timestamp") }} as create_timestamp,
            {{ bronze_inteiro('"order"', "smallint") }} as "order",
            _fatia
        from {{ source("mapas", "bronze_metalist") }}
    ),

    deduplicado as (
        select *, row_number() over (partition by id order by _fatia desc) as ordem
        from tipado
        where id is not null
    )

select
    id, object_type, object_id, grp, title, description, value, create_timestamp, "order"
from deduplicado
where ordem = 1
