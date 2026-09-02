-- Bronze SALIC — sac__tbprogramarouanetresultadofinal.
-- Origem: salic_bronze.sac__tbprogramarouanetresultadofinal, onde tudo chega como texto
-- da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 9 colunas: 6 tipadas, 2 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idprogramarouanetresultadofinal") }}
    as idprogramarouanetresultadofinal,
    {{ bronze_inteiro("idprogramarouanet") }} as idprogramarouanet,
    {{ bronze_inteiro("idpreprojeto") }} as idpreprojeto,
    {{ bronze_numerico("nrtotalpontos") }} as nrtotalpontos,
    {{ bronze_inteiro("mrpontosextras") }} as mrpontosextras,
    {{ bronze_texto("dsmotivacao") }} as dsmotivacao,
    {{ bronze_inteiro("nrranking") }} as nrranking,
    {{ bronze_texto("siresultadofinal") }} as siresultadofinal,
    _fatia
from {{ source("bronze_sac", "sac__tbprogramarouanetresultadofinal") }}
