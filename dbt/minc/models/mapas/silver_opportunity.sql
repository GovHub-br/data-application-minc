-- Silver Mapas — opportunity: oportunidades (editais e chamadas).
-- Origem: mapas.bronze_opportunity, cópia fiel em que tudo chega como texto.
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
            {{ bronze_inteiro("parent_id") }} as parent_id,
            {{ bronze_inteiro("agent_id") }} as agent_id,
            {{ bronze_inteiro("type", "smallint") }} as type,
            {{ bronze_texto("name") }} as name,
            {{ bronze_texto("short_description") }} as short_description,
            {{ bronze_texto("long_description") }} as long_description,
            {{ bronze_timestamp("registration_from") }} as registration_from,
            {{ bronze_timestamp("registration_to") }} as registration_to,
            {{ bronze_booleano("published_registrations") }} as published_registrations,
            {{ bronze_texto("registration_categories") }} as registration_categories,
            {{ bronze_timestamp("create_timestamp") }} as create_timestamp,
            {{ bronze_timestamp("update_timestamp") }} as update_timestamp,
            {{ bronze_inteiro("status", "smallint") }} as status,
            {{ bronze_inteiro("subsite_id") }} as subsite_id,
            {{ bronze_texto("object_type") }} as object_type,
            {{ bronze_inteiro("object_id") }} as object_id,
            {{ mapas_json("avaliable_evaluation_fields") }}
            as avaliable_evaluation_fields,
            {{ bronze_timestamp("publish_timestamp") }} as publish_timestamp,
            {{ bronze_booleano("auto_publish") }} as auto_publish,
            {{ mapas_json("registration_proponent_types") }}
            as registration_proponent_types,
            {{ mapas_json("registration_ranges") }} as registration_ranges,
            {{ bronze_timestamp("continuous_flow") }} as continuous_flow,
            {{ bronze_booleano("publicity_only") }} as publicity_only,
            _fatia
        from {{ source("mapas", "bronze_opportunity") }}
    ),

    deduplicado as (
        select *, row_number() over (partition by id order by _fatia desc) as ordem
        from tipado
        where id is not null
    )

select
    id,
    parent_id,
    agent_id,
    type,
    name,
    short_description,
    long_description,
    registration_from,
    registration_to,
    published_registrations,
    registration_categories,
    create_timestamp,
    update_timestamp,
    status,
    subsite_id,
    object_type,
    object_id,
    avaliable_evaluation_fields,
    publish_timestamp,
    auto_publish,
    registration_proponent_types,
    registration_ranges,
    continuous_flow,
    publicity_only
from deduplicado
where ordem = 1
