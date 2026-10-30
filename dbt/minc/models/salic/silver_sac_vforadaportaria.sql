-- Silver SALIC — silver_sac_vforadaportaria (ex-sac__vforadaportaria).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_vforadaportaria, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 9 colunas: 2 tipadas, 6 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_texto("sequencial") }} as sequencial,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_texto("tipoaprovacao") }} as tipoaprovacao,
    {{ bronze_timestamp("dtaprovacao") }} as dtaprovacao,
    {{ bronze_inteiro("diasvencido") }} as diasvencido,
    {{ bronze_texto("situacao") }} as situacao,
    {{ bronze_texto("orgao") }} as orgao,
    _fatia
from {{ source("salic", "bronze_sac_vforadaportaria") }}
