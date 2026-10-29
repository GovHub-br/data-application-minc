-- Silver Mapas — chat_message: mensagens do chat da plataforma.
-- Origem: mapas.bronze_chat_message, cópia fiel em que tudo chega como texto.
-- Esta camada só limpa, tipa e tira duplicata; não cruza tabelas nem fontes.
--
-- Grão: 1 linha por `id`.
--
-- Tipos da origem, mas id e código em bigint (R17). Texto vazio e só de espaços viram
-- NULL;
-- valor que não casa com o tipo vira NULL; ausente fica nulo, nunca vira falso.
with
    tipado as (
        select
            {{ bronze_inteiro("id", "bigint") }} as id,
            {{ bronze_inteiro("chat_thread_id", "bigint") }} as chat_thread_id,
            {{ bronze_inteiro("parent_id", "bigint") }} as parent_id,
            {{ bronze_inteiro("user_id", "bigint") }} as user_id,
            {{ mapas_json("payload") }} as payload,
            {{ bronze_timestamp("create_timestamp") }} as create_timestamp,
            _fatia
        from {{ source("mapas", "bronze_chat_message") }}
    ),

    deduplicado as (
        select *, row_number() over (partition by id order by _fatia desc) as ordem
        from tipado
        where id is not null
    )

select id, chat_thread_id, parent_id, user_id, payload, create_timestamp
from deduplicado
where ordem = 1
