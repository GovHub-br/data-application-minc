-- Bronze SALIC — sac__captacaoconversao.
-- Origem: salic_bronze.sac__captacaoconversao, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 7 colunas: 4 tipadas, 2 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_texto("sequencial") }} as sequencial,
    {{ bronze_inteiro("numerorecibo") }} as numerorecibo,
    {{ bronze_timestamp("dtconversao") }} as dtconversao,
    {{ bronze_numerico("valor") }} as valor,
    {{ bronze_inteiro("logon") }} as logon,
    _fatia
from {{ source("bronze_sac", "sac__captacaoconversao") }}
