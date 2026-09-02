-- Bronze SALIC — sac__tblote.
-- Origem: salic_bronze.sac__tblote, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 3 colunas: 2 tipadas, 0 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idlote") }} as idlote,
    {{ bronze_timestamp("dtlote") }} as dtlote,
    _fatia
from {{ source("bronze_sac", "sac__tblote") }}
