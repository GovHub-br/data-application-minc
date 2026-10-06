{{ config(alias="silver_registration") }}

-- Silver Mapas — registration: inscrições nas oportunidades.
-- Origem: mapas.bronze_registration, cópia fiel em que tudo chega como texto.
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
            {{ bronze_inteiro("opportunity_id") }} as opportunity_id,
            {{ bronze_texto("category") }} as category,
            {{ bronze_inteiro("agent_id") }} as agent_id,
            {{ bronze_timestamp("create_timestamp") }} as create_timestamp,
            {{ bronze_timestamp("sent_timestamp") }} as sent_timestamp,
            {{ bronze_inteiro("status", "smallint") }} as status,
            {{ bronze_texto("agents_data") }} as agents_data,
            {{ bronze_inteiro("subsite_id") }} as subsite_id,
            {{ bronze_texto("consolidated_result") }} as consolidated_result,
            {{ bronze_texto("number") }} as number,
            {{ mapas_json("valuers_exceptions_list") }} as valuers_exceptions_list,
            {{ bronze_texto("space_data") }} as space_data,
            {{ bronze_texto("proponent_type") }} as proponent_type,
            {{ bronze_texto("range") }} as range,
            {{ mapas_float("score") }} as score,
            {{ bronze_booleano("eligible") }} as eligible,
            {{ bronze_timestamp("editable_until") }} as editable_until,
            {{ bronze_timestamp("edit_sent_timestamp") }} as edit_sent_timestamp,
            {{ mapas_json("editable_fields") }} as editable_fields,
            {{ bronze_timestamp("update_timestamp") }} as update_timestamp,
            {{ mapas_json("valuers") }} as valuers,
            _fatia
        from {{ source("mapas", "bronze_registration") }}
    ),

    deduplicado as (
        select *, row_number() over (partition by id order by _fatia desc) as ordem
        from tipado
        where id is not null
    )

select
    id,
    opportunity_id,
    category,
    agent_id,
    create_timestamp,
    sent_timestamp,
    status,
    agents_data,
    subsite_id,
    consolidated_result,
    number,
    valuers_exceptions_list,
    space_data,
    proponent_type,
    range,
    score,
    eligible,
    editable_until,
    edit_sent_timestamp,
    editable_fields,
    update_timestamp,
    valuers
from deduplicado
where ordem = 1
