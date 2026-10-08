-- Silver Mapas — term_relation: termos associados a objetos.
-- Origem: mapas.bronze_term_relation, cópia fiel em que tudo chega como texto.
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
            {{ bronze_inteiro("term_id") }} as term_id,
            {{ bronze_texto("object_type") }} as object_type,
            {{ bronze_inteiro("object_id") }} as object_id,
            {{ bronze_inteiro("id") }} as id,
            _fatia
        from {{ source("mapas", "bronze_term_relation") }}
    ),

    deduplicado as (
        select *, row_number() over (partition by id order by _fatia desc) as ordem
        from tipado
        where id is not null
    )

select term_id, object_type, object_id, id
from deduplicado
where ordem = 1
