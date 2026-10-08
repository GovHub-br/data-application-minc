-- Silver Mapas — registration_workplan_goal_delivery: entregas das metas dos planos de
-- trabalho.
-- Origem: mapas.bronze_registration_workplan_goal_delivery, cópia fiel em que tudo
-- chega como texto.
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
            {{ bronze_inteiro("goal_id") }} as goal_id,
            {{ bronze_timestamp("create_timestamp") }} as create_timestamp,
            {{ bronze_timestamp("update_timestamp") }} as update_timestamp,
            {{ bronze_inteiro("status", "smallint") }} as status,
            _fatia
        from {{ source("mapas", "bronze_registration_workplan_goal_delivery") }}
    ),

    deduplicado as (
        select *, row_number() over (partition by id order by _fatia desc) as ordem
        from tipado
        where id is not null
    )

select id, agent_id, goal_id, create_timestamp, update_timestamp, status
from deduplicado
where ordem = 1
