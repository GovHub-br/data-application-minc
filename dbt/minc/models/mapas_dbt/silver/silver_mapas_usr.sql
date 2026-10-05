{{ config(alias="silver_usr") }}

-- Silver Mapas — usr: usuários da plataforma.
-- Origem: mapas.bronze_usr, cópia fiel em que tudo chega como texto.
-- Esta camada só limpa, tipa e tira duplicata; não cruza tabelas nem fontes.
--
-- Grão: 1 linha por usuário (`id`). `agent.user_id` aponta para cá.
--
-- FORA DESTA TABELA, DE PROPÓSITO: `auth_uid` (identificador de login no
-- provedor de autenticação, que pode ser o CPF) e `email`. São identificadores
-- pessoais sem uso analítico, e a silver é lida por mais gente do que a bronze.
-- Quem precisar deles volta à bronze; acrescentar uma coluna depois é fácil,
-- retirar uma que já foi lida não é.
--
-- `last_login_timestamp` é NOT NULL na origem, e não há data-sentinela nela
-- (conferido por agregado no banco real). Uma parte dos usuários tem o último
-- login igual à criação: é dado, não ausência, e fica como está.
--
-- Duplicata: a origem tem PK em `id`; fica a linha da fatia mais recente.
with
    tipado as (
        select
            {{ bronze_inteiro("id") }} as id,
            {{ bronze_inteiro("auth_provider", "smallint") }} as auth_provider,
            {{ bronze_timestamp("last_login_timestamp") }} as last_login_timestamp,
            {{ bronze_timestamp("create_timestamp") }} as create_timestamp,
            {{ bronze_inteiro("status", "smallint") }} as status,
            {{ bronze_inteiro("profile_id") }} as profile_id,
            _fatia
        from {{ source("mapas", "bronze_usr") }}
    ),

    deduplicado as (
        select *, row_number() over (partition by id order by _fatia desc) as ordem
        from tipado
        where id is not null
    )

select id, auth_provider, last_login_timestamp, create_timestamp, status, profile_id
from deduplicado
where ordem = 1
