-- Bronze SALIC — sac__edital.
-- Origem: salic_bronze.sac__edital, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 15 colunas: 10 tipadas, 4 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idedital") }} as idedital,
    {{ bronze_inteiro("idorgao") }} as idorgao,
    {{ bronze_inteiro("nredital") }} as nredital,
    {{ bronze_timestamp("dtedital") }} as dtedital,
    {{ bronze_texto("celulaorcamentaria") }} as celulaorcamentaria,
    {{ bronze_texto("objeto") }} as objeto,
    {{ bronze_inteiro("logon") }} as logon,
    {{ bronze_inteiro("qtavaliador") }} as qtavaliador,
    {{ bronze_texto("stdistribuicao") }} as stdistribuicao,
    {{ bronze_texto("stadmissibilidade") }} as stadmissibilidade,
    {{ bronze_inteiro("cdtipofundo") }} as cdtipofundo,
    {{ bronze_inteiro("idati") }} as idati,
    {{ bronze_inteiro("idlinguagem") }} as idlinguagem,
    {{ bronze_inteiro("idmodalidade") }} as idmodalidade,
    _fatia
from {{ source("bronze_sac", "sac__edital") }}
