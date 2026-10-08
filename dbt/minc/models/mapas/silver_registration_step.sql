-- Silver Mapas — registration_step: etapas das oportunidades.
-- Origem: mapas.bronze_registration_step, cópia fiel em que tudo chega como texto.
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
            {{ bronze_texto("name") }} as name,
            {{ bronze_inteiro("display_order") }} as display_order,
            {{ bronze_inteiro("opportunity_id") }} as opportunity_id,
            {{ bronze_timestamp("create_timestamp") }} as create_timestamp,
            {{ bronze_timestamp("update_timestamp") }} as update_timestamp,
            {{ mapas_json("metadata") }} as metadata,
            _fatia
        from {{ source("mapas", "bronze_registration_step") }}
    ),

    deduplicado as (
        select *, row_number() over (partition by id order by _fatia desc) as ordem
        from tipado
        where id is not null
    )

select
    id, name, display_order, opportunity_id, create_timestamp, update_timestamp, metadata
from deduplicado
where ordem = 1
