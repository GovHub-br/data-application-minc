-- Silver Mapas — registration_workplan: planos de trabalho das inscrições.
-- Origem: mapas.bronze_registration_workplan, cópia fiel em que tudo chega como texto.
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
            {{ bronze_inteiro("agent_id") }} as agent_id,
            {{ bronze_inteiro("registration_id") }} as registration_id,
            {{ bronze_timestamp("create_timestamp") }} as create_timestamp,
            {{ bronze_timestamp("update_timestamp") }} as update_timestamp,
            _fatia
        from {{ source("mapas", "bronze_registration_workplan") }}
    ),

    deduplicado as (
        select *, row_number() over (partition by id order by _fatia desc) as ordem
        from tipado
        where id is not null
    )

select id, agent_id, registration_id, create_timestamp, update_timestamp
from deduplicado
where ordem = 1
