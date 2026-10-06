{{ config(alias="silver_metadata") }}

-- Silver Mapas — metadata: metadados genéricos de objetos, em chave-valor.
-- Origem: mapas.bronze_metadata, cópia fiel em que tudo chega como texto.
-- Esta camada só limpa, tipa e tira duplicata; não cruza tabelas nem fontes.
--
-- Grão: 1 linha por `object_id`, `object_type`, `key`.
--
-- Tipagem pelos tipos que o banco do Mapas declara. Texto vazio e só de espaços viram
-- NULL;
-- valor que não casa com o tipo vira NULL; ausente fica nulo, nunca vira falso.
with
    tipado as (
        select
            {{ bronze_inteiro("object_id") }} as object_id,
            {{ bronze_texto("object_type") }} as object_type,
            {{ bronze_texto("key") }} as key,
            {{ bronze_texto("value") }} as value,
            _fatia
        from {{ source("mapas", "bronze_metadata") }}
    ),

    deduplicado as (
        select
            *,
            row_number() over (
                partition by object_id, object_type, key order by _fatia desc
            ) as ordem
        from tipado
        where object_id is not null and object_type is not null and key is not null
    )

select object_id, object_type, key, value
from deduplicado
where ordem = 1
