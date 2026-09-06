-- Bronze SALIC — sac__empenho.
-- Origem: salic_bronze.sac__empenho, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 12 colunas: 4 tipadas, 7 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("contador") }} as contador,
    {{ bronze_texto("uggestao") }} as uggestao,
    {{ bronze_texto("ugr") }} as ugr,
    {{ bronze_texto("ptres") }} as ptres,
    {{ bronze_texto("programatrabalho") }} as programatrabalho,
    {{ bronze_texto("fonterecurso") }} as fonterecurso,
    {{ bronze_texto("naturezadespesa") }} as naturezadespesa,
    {{ bronze_texto("nrempenho") }} as nrempenho,
    {{ bronze_timestamp("dtempenho") }} as dtempenho,
    {{ bronze_numerico("valor") }} as valor,
    {{ bronze_inteiro("logon") }} as logon,
    _fatia
from {{ source("bronze_sac", "sac__empenho") }}
