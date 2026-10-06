{{ config(alias="silver_security_allowlist") }}

-- Silver Mapas — security_allowlist: endereços IP permitidos.
-- Origem: mapas.bronze_security_allowlist, cópia fiel em que tudo chega como texto.
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
            {{ bronze_texto("ip") }} as ip,
            {{ bronze_texto("origin") }} as origin,
            {{ bronze_texto("note") }} as note,
            {{ bronze_timestamp("created_at") }} as created_at,
            {{ bronze_inteiro("created_by") }} as created_by,
            {{ bronze_timestamp("removed_at") }} as removed_at,
            {{ bronze_inteiro("removed_by") }} as removed_by,
            {{ bronze_texto("removal_note") }} as removal_note,
            _fatia
        from {{ source("mapas", "bronze_security_allowlist") }}
    ),

    deduplicado as (
        select *, row_number() over (partition by id order by _fatia desc) as ordem
        from tipado
        where id is not null
    )

select id, ip, origin, note, created_at, created_by, removed_at, removed_by, removal_note
from deduplicado
where ordem = 1
