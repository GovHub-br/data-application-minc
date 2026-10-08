-- Silver Mapas — chat_thread: conversas do chat.
-- Origem: mapas.bronze_chat_thread, cópia fiel em que tudo chega como texto.
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
            {{ bronze_inteiro("object_id") }} as object_id,
            {{ bronze_texto("object_type") }} as object_type,
            {{ bronze_texto("type") }} as type,
            {{ bronze_texto("identifier") }} as identifier,
            {{ bronze_texto("description") }} as description,
            {{ bronze_timestamp("create_timestamp") }} as create_timestamp,
            {{ bronze_timestamp("last_message_timestamp") }} as last_message_timestamp,
            {{ bronze_inteiro("status") }} as status,
            _fatia
        from {{ source("mapas", "bronze_chat_thread") }}
    ),

    deduplicado as (
        select *, row_number() over (partition by id order by _fatia desc) as ordem
        from tipado
        where id is not null
    )

select
    id,
    object_id,
    object_type,
    type,
    identifier,
    description,
    create_timestamp,
    last_message_timestamp,
    status
from deduplicado
where ordem = 1
