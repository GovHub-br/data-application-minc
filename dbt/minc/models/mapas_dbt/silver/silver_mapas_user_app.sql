{{ config(alias="silver_user_app") }}

-- Silver Mapas — user_app: aplicações (chaves de API) cadastradas, sem a chave privada.
-- Origem: mapas.bronze_user_app, cópia fiel em que tudo chega como texto.
-- Esta camada só limpa, tipa e tira duplicata; não cruza tabelas nem fontes.
--
-- Grão: 1 linha por `public_key`.
--
-- Tipagem pelos tipos que o banco do Mapas declara. Texto vazio e só de espaços viram
-- NULL;
-- valor que não casa com o tipo vira NULL; ausente fica nulo, nunca vira falso.
--
-- FORA DESTA TABELA, DE PROPÓSITO: `private_key` (chave privada da aplicação
-- (credencial)). A silver é lida por mais gente do que a bronze.
with
    tipado as (
        select
            {{ bronze_texto("public_key") }} as public_key,
            {{ bronze_inteiro("user_id") }} as user_id,
            {{ bronze_texto("name") }} as name,
            {{ bronze_inteiro("status") }} as status,
            {{ bronze_timestamp("create_timestamp") }} as create_timestamp,
            {{ bronze_inteiro("subsite_id") }} as subsite_id,
            _fatia
        from {{ source("mapas", "bronze_user_app") }}
    ),

    deduplicado as (
        select
            *, row_number() over (partition by public_key order by _fatia desc) as ordem
        from tipado
        where public_key is not null
    )

select public_key, user_id, name, status, create_timestamp, subsite_id
from deduplicado
where ordem = 1
