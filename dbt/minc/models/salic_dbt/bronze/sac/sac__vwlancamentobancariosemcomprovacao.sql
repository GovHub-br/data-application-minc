-- Bronze SALIC — sac__vwlancamentobancariosemcomprovacao.
-- Origem: salic_bronze.sac__vwlancamentobancariosemcomprovacao, onde tudo chega como
-- texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 9 colunas: 4 tipadas, 4 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_inteiro("pronac") }} as pronac,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_texto("cdlancamento") }} as cdlancamento,
    {{ bronze_texto("dslancamento") }} as dslancamento,
    {{ bronze_texto("nrlancamento") }} as nrlancamento,
    {{ bronze_timestamp("dtlancamento") }} as dtlancamento,
    {{ bronze_numerico("vllancamento") }} as vllancamento,
    _fatia
from {{ source("bronze_sac", "sac__vwlancamentobancariosemcomprovacao") }}
