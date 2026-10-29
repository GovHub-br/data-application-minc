-- Silver Mapas — evaluationmethodconfiguration_meta: atributos da configuração do
-- método de avaliação, em chave-valor.
-- Origem: mapas.bronze_evaluationmethodconfiguration_meta, cópia fiel em que tudo chega
-- como texto.
-- Esta camada só limpa, tipa e tira duplicata; não cruza tabelas nem fontes.
--
-- Grão: 1 linha por `id`.
--
-- Tipos da origem, mas id e código em bigint (R17). Texto vazio e só de espaços viram
-- NULL;
-- valor que não casa com o tipo vira NULL; ausente fica nulo, nunca vira falso.
with
    tipado as (
        select
            {{ bronze_inteiro("id", "bigint") }} as id,
            {{ bronze_inteiro("object_id", "bigint") }} as object_id,
            {{ bronze_texto("key") }} as key,
            {{ bronze_texto("value") }} as value,
            _fatia
        from {{ source("mapas", "bronze_evaluationmethodconfiguration_meta") }}
    ),

    deduplicado as (
        select *, row_number() over (partition by id order by _fatia desc) as ordem
        from tipado
        where id is not null
    )

select id, object_id, key, value
from deduplicado
where ordem = 1
