-- Silver Mapas — procuration: procurações: um usuário autorizado a agir em nome de outro.
-- Origem: mapas.bronze_procuration, cópia fiel em que tudo chega como texto.
-- Esta camada só limpa, tipa e tira duplicata; não cruza tabelas nem fontes.
--
-- Grão: a chave primária da origem é a coluna excluída abaixo; duplicata aqui é linha
-- idêntica.
--
-- Tipagem pelos tipos que o banco do Mapas declara. Texto vazio e só de espaços viram
-- NULL;
-- valor que não casa com o tipo vira NULL; ausente fica nulo, nunca vira falso.
--
-- FORA DESTA TABELA, DE PROPÓSITO: `token` (token da procuração (credencial)). A silver
-- é lida por mais gente do que a bronze.
with
    tipado as (
        select
            {{ bronze_inteiro("usr_id") }} as usr_id,
            {{ bronze_inteiro("attorney_user_id") }} as attorney_user_id,
            {{ bronze_texto("action") }} as action,
            {{ bronze_timestamp("create_timestamp") }} as create_timestamp,
            {{ bronze_timestamp("valid_until_timestamp") }} as valid_until_timestamp
        from {{ source("mapas", "bronze_procuration") }}
    )

select distinct usr_id, attorney_user_id, action, create_timestamp, valid_until_timestamp
from tipado
