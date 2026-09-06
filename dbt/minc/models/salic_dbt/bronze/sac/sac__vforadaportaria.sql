-- Bronze SALIC — sac__vforadaportaria.
-- Origem: salic_bronze.sac__vforadaportaria, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 9 colunas: 0 tipadas, 8 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_texto("sequencial") }} as sequencial,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_texto("tipoaprovacao") }} as tipoaprovacao,
    {{ bronze_texto("dtaprovacao") }} as dtaprovacao,
    {{ bronze_texto("diasvencido") }} as diasvencido,
    {{ bronze_texto("situacao") }} as situacao,
    {{ bronze_texto("orgao") }} as orgao,
    _fatia
from {{ source("bronze_sac", "sac__vforadaportaria") }}
