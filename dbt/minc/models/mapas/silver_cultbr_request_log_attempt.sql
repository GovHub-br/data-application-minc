-- Silver Mapas — cultbr_request_log_attempt: tentativas de cada chamada à integração
-- CultBR.
-- Origem: mapas.bronze_cultbr_request_log_attempt, cópia fiel em que tudo chega como
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
            {{ bronze_inteiro("log_id") }} as log_id,
            {{ bronze_inteiro("attempt") }} as attempt,
            {{ bronze_inteiro("max_attempts") }} as max_attempts,
            {{ bronze_texto("endpoint") }} as endpoint,
            {{ bronze_texto("http_method") }} as http_method,
            {{ bronze_inteiro("http_status") }} as http_status,
            {{ mapas_json("payload") }} as payload,
            {{ mapas_json("response") }} as response,
            {{ bronze_texto("error_message") }} as error_message,
            {{ bronze_texto("status") }} as status,
            {{ bronze_timestamp("sent_at") }} as sent_at,
            {{ bronze_inteiro("duration_ms") }} as duration_ms,
            {{ mapas_json("response_headers") }} as response_headers,
            _fatia
        from {{ source("mapas", "bronze_cultbr_request_log_attempt") }}
    ),

    deduplicado as (
        select *, row_number() over (partition by id order by _fatia desc) as ordem
        from tipado
        where id is not null
    )

select
    id,
    log_id,
    attempt,
    max_attempts,
    endpoint,
    http_method,
    http_status,
    payload,
    response,
    error_message,
    status,
    sent_at,
    duration_ms,
    response_headers
from deduplicado
where ordem = 1
