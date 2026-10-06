{{ config(alias="silver_registration_evaluation") }}

-- Silver Mapas — registration_evaluation: avaliações das inscrições.
-- Origem: mapas.bronze_registration_evaluation, cópia fiel em que tudo chega como texto.
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
            {{ bronze_inteiro("registration_id") }} as registration_id,
            {{ bronze_inteiro("user_id") }} as user_id,
            {{ bronze_texto("result") }} as result,
            {{ bronze_texto("evaluation_data") }} as evaluation_data,
            {{ bronze_inteiro("status", "smallint") }} as status,
            {{ bronze_timestamp("create_timestamp") }} as create_timestamp,
            {{ bronze_timestamp("update_timestamp") }} as update_timestamp,
            {{ bronze_timestamp("sent_timestamp") }} as sent_timestamp,
            {{ bronze_booleano("is_tiebreaker") }} as is_tiebreaker,
            {{ bronze_texto("committee") }} as committee,
            _fatia
        from {{ source("mapas", "bronze_registration_evaluation") }}
    ),

    deduplicado as (
        select *, row_number() over (partition by id order by _fatia desc) as ordem
        from tipado
        where id is not null
    )

select
    id,
    registration_id,
    user_id,
    result,
    evaluation_data,
    status,
    create_timestamp,
    update_timestamp,
    sent_timestamp,
    is_tiebreaker,
    committee
from deduplicado
where ordem = 1
