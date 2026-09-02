-- Bronze SALIC — sac__vwpaineladmissibilidade.
-- Origem: salic_bronze.sac__vwpaineladmissibilidade, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 21 colunas: 10 tipadas, 10 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idprojeto") }} as idprojeto,
    {{ bronze_texto("nmproposta") }} as nmproposta,
    {{ bronze_inteiro("idagente") }} as idagente,
    {{ bronze_texto("nrcnpjcpf") }} as nrcnpjcpf,
    {{ bronze_texto("nmproponente") }} as nmproponente,
    {{ bronze_inteiro("cdtipodeexecucao") }} as cdtipodeexecucao,
    {{ bronze_texto("dstipodeexecucao") }} as dstipodeexecucao,
    {{ bronze_inteiro("cdtipicidade") }} as cdtipicidade,
    {{ bronze_texto("dstipicidade") }} as dstipicidade,
    {{ bronze_inteiro("cdtipologia") }} as cdtipologia,
    {{ bronze_texto("dstipologia") }} as dstipologia,
    {{ bronze_texto("cdmovimentacao") }} as cdmovimentacao,
    {{ bronze_inteiro("cdconformidade") }} as cdconformidade,
    {{ bronze_texto("sianalise") }} as sianalise,
    {{ bronze_inteiro("idtecnico") }} as idtecnico,
    {{ bronze_texto("nmtecnico") }} as nmtecnico,
    {{ bronze_timestamp("dtenvioproposta") }} as dtenvioproposta,
    {{ bronze_inteiro("qtdiasemanalise") }} as qtdiasemanalise,
    {{ bronze_numerico("vlproposta") }} as vlproposta,
    {{ bronze_texto("idsecretaria") }} as idsecretaria,
    _fatia
from {{ source("bronze_sac", "sac__vwpaineladmissibilidade") }}
