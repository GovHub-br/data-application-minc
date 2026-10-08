-- Silver Mapas — project_event: relação entre projetos e eventos.
-- Origem: mapas.bronze_project_event, cópia fiel em que tudo chega como texto.
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
            {{ bronze_inteiro("event_id") }} as event_id,
            {{ bronze_inteiro("project_id") }} as project_id,
            {{ bronze_inteiro("type", "smallint") }} as type,
            {{ bronze_inteiro("status", "smallint") }} as status,
            _fatia
        from {{ source("mapas", "bronze_project_event") }}
    ),

    deduplicado as (
        select *, row_number() over (partition by id order by _fatia desc) as ordem
        from tipado
        where id is not null
    )

select id, event_id, project_id, type, status
from deduplicado
where ordem = 1
