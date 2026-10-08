-- Silver Mapas — event_attendance: presenças confirmadas em ocorrências de eventos.
-- Origem: mapas.bronze_event_attendance, cópia fiel em que tudo chega como texto.
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
            {{ bronze_inteiro("user_id") }} as user_id,
            {{ bronze_inteiro("event_occurrence_id") }} as event_occurrence_id,
            {{ bronze_inteiro("event_id") }} as event_id,
            {{ bronze_inteiro("space_id") }} as space_id,
            {{ bronze_texto("type") }} as type,
            {{ bronze_texto("reccurrence_string") }} as reccurrence_string,
            {{ bronze_timestamp("start_timestamp") }} as start_timestamp,
            {{ bronze_timestamp("end_timestamp") }} as end_timestamp,
            {{ bronze_timestamp("create_timestamp") }} as create_timestamp,
            _fatia
        from {{ source("mapas", "bronze_event_attendance") }}
    ),

    deduplicado as (
        select *, row_number() over (partition by id order by _fatia desc) as ordem
        from tipado
        where id is not null
    )

select
    id,
    user_id,
    event_occurrence_id,
    event_id,
    space_id,
    type,
    reccurrence_string,
    start_timestamp,
    end_timestamp,
    create_timestamp
from deduplicado
where ordem = 1
