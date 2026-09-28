-- Bronze SALIC — agentes__vwpropostasemavaliacao.
-- Origem: salic_bronze.agentes__vwpropostasemavaliacao, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 17 colunas: 7 tipadas, 9 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idpreprojeto") }} as idpreprojeto,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_inteiro("tptipicidade") }} as tptipicidade,
    {{ bronze_texto("dstipicidade") }} as dstipicidade,
    {{ bronze_inteiro("tptipologia") }} as tptipologia,
    {{ bronze_texto("dstipologia") }} as dstipologia,
    {{ bronze_inteiro("stproposta") }} as stproposta,
    {{ bronze_texto("dsplanoexecucaoimediata") }} as dsplanoexecucaoimediata,
    {{ bronze_texto("secretaria") }} as secretaria,
    {{ bronze_inteiro("ano") }} as ano,
    {{ bronze_texto("conformidade") }} as conformidade,
    {{ bronze_inteiro("idtecnico") }} as idtecnico,
    {{ bronze_texto("nmtecnico") }} as nmtecnico,
    {{ bronze_inteiro("cdproponente") }} as cdproponente,
    {{ bronze_texto("cnpjcpf") }} as cnpjcpf,
    {{ bronze_texto("nmproponente") }} as nmproponente,
    _fatia
from {{ source("bronze_agentes", "agentes__vwpropostasemavaliacao") }}
