-- Bronze SALIC — sac__enquadramento.
-- Origem: salic_bronze.sac__enquadramento, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 9 colunas: 5 tipadas, 3 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idenquadramento") }} as idenquadramento,
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_texto("sequencial") }} as sequencial,
    {{ bronze_inteiro("enquadramento") }} as enquadramento,
    {{ bronze_timestamp("dtenquadramento") }} as dtenquadramento,
    {{ bronze_texto("observacao") }} as observacao,
    {{ bronze_inteiro("logon") }} as logon,
    {{ bronze_inteiro("idpronac") }} as idpronac,
    _fatia
from {{ source("bronze_sac", "sac__enquadramento") }}
