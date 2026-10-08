-- Silver Mapas — event_occurrence_cancellation: cancelamentos de ocorrências de eventos.
-- Origem: mapas.bronze_event_occurrence_cancellation, cópia fiel em que tudo chega como
-- texto.
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
            {{ bronze_inteiro("event_occurrence_id") }} as event_occurrence_id,
            {{ bronze_data("date") }} as date,
            _fatia
        from {{ source("mapas", "bronze_event_occurrence_cancellation") }}
    ),

    deduplicado as (
        select *, row_number() over (partition by id order by _fatia desc) as ordem
        from tipado
        where id is not null
    )

select id, event_occurrence_id, date
from deduplicado
where ordem = 1
