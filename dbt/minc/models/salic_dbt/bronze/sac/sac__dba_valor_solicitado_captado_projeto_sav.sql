-- Bronze SALIC — sac__dba_valor_solicitado_captado_projeto_sav.
-- Origem: salic_bronze.sac__dba_valor_solicitado_captado_projeto_sav, onde tudo chega
-- como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 7 colunas: 2 tipadas, 4 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_texto("sequencial") }} as sequencial,
    {{ bronze_texto("situacao") }} as situacao,
    {{ bronze_texto("ano_protocolo") }} as ano_protocolo,
    {{ bronze_numerico("valor_aprovado") }} as valor_aprovado,
    {{ bronze_numerico("valor_captado") }} as valor_captado,
    _fatia
from {{ source("bronze_sac", "sac__dba_valor_solicitado_captado_projeto_sav") }}
