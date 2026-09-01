-- Bronze SALIC — sac__consultorespc.
-- Origem: salic_bronze.sac__consultorespc, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 12 colunas: 8 tipadas, 3 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("contador") }} as contador,
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_texto("sequencial") }} as sequencial,
    {{ bronze_timestamp("dtsaida") }} as dtsaida,
    {{ bronze_timestamp("dtretorno") }} as dtretorno,
    {{ bronze_inteiro("consultor") }} as consultor,
    {{ bronze_texto("observacao") }} as observacao,
    {{ bronze_inteiro("logon") }} as logon,
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_inteiro("idagente") }} as idagente,
    {{ bronze_inteiro("qtdevolume") }} as qtdevolume,
    _fatia
from {{ source("bronze_sac", "sac__consultorespc") }}
