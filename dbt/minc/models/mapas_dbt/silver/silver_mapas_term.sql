{{ config(alias="silver_term") }}

-- Silver Mapas — term: termos de taxonomia (áreas, linguagens, tags).
-- Origem: mapas.bronze_term, cópia fiel em que tudo chega como texto.
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
            {{ bronze_texto("taxonomy") }} as taxonomy,
            {{ bronze_texto("term") }} as term,
            {{ bronze_texto("description") }} as description,
            _fatia
        from {{ source("mapas", "bronze_term") }}
    ),

    deduplicado as (
        select *, row_number() over (partition by id order by _fatia desc) as ordem
        from tipado
        where id is not null
    )

select id, taxonomy, term, description
from deduplicado
where ordem = 1
