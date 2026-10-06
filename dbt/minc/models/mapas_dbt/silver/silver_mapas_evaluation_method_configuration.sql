{{ config(alias="silver_evaluation_method_configuration") }}

-- Silver Mapas — evaluation_method_configuration: configuração do método de avaliação
-- de uma oportunidade.
-- Origem: mapas.bronze_evaluation_method_configuration, cópia fiel em que tudo chega
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
            {{ bronze_texto("type") }} as type,
            {{ bronze_timestamp("evaluation_from") }} as evaluation_from,
            {{ bronze_timestamp("evaluation_to") }} as evaluation_to,
            {{ bronze_texto("name") }} as name,
            _fatia
        from {{ source("mapas", "bronze_evaluation_method_configuration") }}
    ),

    deduplicado as (
        select *, row_number() over (partition by id order by _fatia desc) as ordem
        from tipado
        where id is not null
    )

select id, opportunity_id, type, evaluation_from, evaluation_to, name
from deduplicado
where ordem = 1
