{{ config(alias="silver_request") }}

-- Silver Mapas — request: solicitações de aprovação entre objetos.
-- Origem: mapas.bronze_request, cópia fiel em que tudo chega como texto.
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
            {{ bronze_texto("request_uid") }} as request_uid,
            {{ bronze_inteiro("requester_user_id") }} as requester_user_id,
            {{ bronze_texto("origin_type") }} as origin_type,
            {{ bronze_inteiro("origin_id") }} as origin_id,
            {{ bronze_texto("destination_type") }} as destination_type,
            {{ bronze_inteiro("destination_id") }} as destination_id,
            {{ bronze_texto("metadata") }} as metadata,
            {{ bronze_texto("type") }} as type,
            {{ bronze_timestamp("create_timestamp") }} as create_timestamp,
            {{ bronze_timestamp("action_timestamp") }} as action_timestamp,
            {{ bronze_inteiro("status", "smallint") }} as status,
            _fatia
        from {{ source("mapas", "bronze_request") }}
    ),

    deduplicado as (
        select *, row_number() over (partition by id order by _fatia desc) as ordem
        from tipado
        where id is not null
    )

select
    id,
    request_uid,
    requester_user_id,
    origin_type,
    origin_id,
    destination_type,
    destination_id,
    metadata,
    type,
    create_timestamp,
    action_timestamp,
    status
from deduplicado
where ordem = 1
