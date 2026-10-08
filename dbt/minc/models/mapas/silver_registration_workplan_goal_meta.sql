-- Silver Mapas — registration_workplan_goal_meta: atributos das metas, em chave-valor.
-- Origem: mapas.bronze_registration_workplan_goal_meta, cópia fiel em que tudo chega
-- como texto.
-- Esta camada só limpa, tipa e tira duplicata; não cruza tabelas nem fontes.
--
-- Grão: sem chave primária utilizável; duplicata aqui é linha idêntica.
--
-- Tipagem pelos tipos que o banco do Mapas declara. Texto vazio e só de espaços viram
-- NULL;
-- valor que não casa com o tipo vira NULL; ausente fica nulo, nunca vira falso.
with
    tipado as (
        select
            {{ bronze_inteiro("object_id") }} as object_id,
            {{ bronze_texto("key") }} as key,
            {{ bronze_texto("value") }} as value,
            {{ bronze_inteiro("id") }} as id
        from {{ source("mapas", "bronze_registration_workplan_goal_meta") }}
    )

select distinct object_id, key, value, id
from tipado
