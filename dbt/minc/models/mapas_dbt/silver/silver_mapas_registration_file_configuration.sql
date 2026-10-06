{{ config(alias="silver_registration_file_configuration") }}

-- Silver Mapas — registration_file_configuration: configuração dos anexos exigidos na
-- inscrição.
-- Origem: mapas.bronze_registration_file_configuration, cópia fiel em que tudo chega
-- como texto.
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
            {{ bronze_texto("title") }} as title,
            {{ bronze_texto("description") }} as description,
            {{ bronze_booleano("required") }} as required,
            {{ bronze_texto("categories") }} as categories,
            {{ bronze_inteiro("display_order", "smallint") }} as display_order,
            {{ bronze_booleano("conditional") }} as conditional,
            {{ bronze_texto("conditional_field") }} as conditional_field,
            {{ bronze_texto("conditional_value") }} as conditional_value,
            {{ mapas_json("registration_ranges") }} as registration_ranges,
            {{ mapas_json("proponent_types") }} as proponent_types,
            {{ bronze_inteiro("step_id") }} as step_id,
            {{ mapas_json("allowed_file_types") }} as allowed_file_types,
            _fatia
        from {{ source("mapas", "bronze_registration_file_configuration") }}
    ),

    deduplicado as (
        select *, row_number() over (partition by id order by _fatia desc) as ordem
        from tipado
        where id is not null
    )

select
    id,
    opportunity_id,
    title,
    description,
    required,
    categories,
    display_order,
    conditional,
    conditional_field,
    conditional_value,
    registration_ranges,
    proponent_types,
    step_id,
    allowed_file_types
from deduplicado
where ordem = 1
