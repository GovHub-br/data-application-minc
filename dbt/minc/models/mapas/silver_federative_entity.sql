-- Silver Mapas — federative_entity: entes federativos (estados e municípios).
-- Origem: mapas.bronze_federative_entity, cópia fiel em que tudo chega como texto.
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
            {{ bronze_texto("document") }} as document,
            {{ bronze_timestamp("create_timestamp") }} as create_timestamp,
            {{ bronze_timestamp("update_timestamp") }} as update_timestamp,
            {{ bronze_inteiro("subsite_id") }} as subsite_id,
            {{ mapas_json("exercices") }} as exercices,
            _fatia
        from {{ source("mapas", "bronze_federative_entity") }}
    ),

    deduplicado as (
        select *, row_number() over (partition by id order by _fatia desc) as ordem
        from tipado
        where id is not null
    )

select id, name, document, create_timestamp, update_timestamp, subsite_id, exercices
from deduplicado
where ordem = 1
