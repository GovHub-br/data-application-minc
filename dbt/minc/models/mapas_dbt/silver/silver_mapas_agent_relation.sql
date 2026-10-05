{{ config(alias="silver_agent_relation") }}

-- Silver Mapas — agent_relation: relação de um agente com outro objeto do Mapas.
-- Origem: mapas.bronze_agent_relation, cópia fiel em que tudo chega como texto.
-- Esta camada só limpa, tipa e tira duplicata; não cruza tabelas nem fontes.
--
-- Grão: 1 linha por relação (`id`).
--
-- `object_type` é o nome da classe do objeto relacionado, por exemplo
-- `MapasCulturais\Entities\Registration`, e `object_id` é o id dele na tabela
-- correspondente. Por isso `object_id` NÃO tem chave estrangeira única: depende
-- do `object_type`. Esta camada não resolve a ligação.
--
-- `metadata` era `json` na origem e sai como `jsonb`. Texto vazio e o literal
-- 'null' viram NULL (macro mapas_json).
--
-- Duplicata: a origem tem PK em `id`; fica a linha da fatia mais recente.
with
    tipado as (
        select
            {{ bronze_inteiro("id") }} as id,
            {{ bronze_inteiro("agent_id") }} as agent_id,
            {{ bronze_texto("object_type") }} as object_type,
            {{ bronze_inteiro("object_id") }} as object_id,
            {{ bronze_texto("type") }} as type,
            {{ bronze_booleano("has_control") }} as has_control,
            {{ bronze_timestamp("create_timestamp") }} as create_timestamp,
            {{ bronze_inteiro("status", "smallint") }} as status,
            {{ mapas_json("metadata") }} as metadata,
            _fatia
        from {{ source("mapas", "bronze_agent_relation") }}
    ),

    deduplicado as (
        select *, row_number() over (partition by id order by _fatia desc) as ordem
        from tipado
        where id is not null
    )

select
    id,
    agent_id,
    object_type,
    object_id,
    type,
    has_control,
    create_timestamp,
    status,
    metadata
from deduplicado
where ordem = 1
