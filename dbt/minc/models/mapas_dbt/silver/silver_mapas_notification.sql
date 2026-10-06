{{ config(alias="silver_notification") }}

-- Silver Mapas — notification: notificações enviadas aos usuários.
-- Origem: mapas.bronze_notification, cópia fiel em que tudo chega como texto.
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
            {{ bronze_inteiro("request_id") }} as request_id,
            {{ bronze_texto("message") }} as message,
            {{ bronze_timestamp("create_timestamp") }} as create_timestamp,
            {{ bronze_timestamp("action_timestamp") }} as action_timestamp,
            {{ bronze_inteiro("status", "smallint") }} as status,
            _fatia
        from {{ source("mapas", "bronze_notification") }}
    ),

    deduplicado as (
        select *, row_number() over (partition by id order by _fatia desc) as ordem
        from tipado
        where id is not null
    )

select id, user_id, request_id, message, create_timestamp, action_timestamp, status
from deduplicado
where ordem = 1
