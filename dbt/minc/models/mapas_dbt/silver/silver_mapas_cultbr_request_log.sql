{{ config(alias="silver_cultbr_request_log") }}

-- Silver Mapas — cultbr_request_log: registro das chamadas à integração CultBR.
-- Origem: mapas.bronze_cultbr_request_log, cópia fiel em que tudo chega como texto.
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
            {{ bronze_texto("request_uuid") }} as request_uuid,
            {{ bronze_inteiro("opportunity_id") }} as opportunity_id,
            {{ bronze_texto("action") }} as action,
            {{ bronze_texto("status") }} as status,
            {{ bronze_timestamp("create_timestamp") }} as create_timestamp,
            {{ bronze_timestamp("update_timestamp") }} as update_timestamp,
            {{ bronze_inteiro("user_id") }} as user_id,
            _fatia
        from {{ source("mapas", "bronze_cultbr_request_log") }}
    ),

    deduplicado as (
        select *, row_number() over (partition by id order by _fatia desc) as ordem
        from tipado
        where id is not null
    )

select
    id,
    request_uuid,
    opportunity_id,
    action,
    status,
    create_timestamp,
    update_timestamp,
    user_id
from deduplicado
where ordem = 1
