-- Bronze SALIC — sac__tbpreprojetometa.
-- Origem: salic_bronze.sac__tbpreprojetometa, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 6 colunas: 3 tipadas, 2 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idpreprojetometa") }} as idpreprojetometa,
    {{ bronze_inteiro("idpreprojeto") }} as idpreprojeto,
    {{ bronze_texto("metakey") }} as metakey,
    {{ bronze_texto("metavalue") }} as metavalue,
    {{ bronze_timestamp("dtalteracao") }} as dtalteracao,
    _fatia
from {{ source("bronze_sac", "sac__tbpreprojetometa") }}
