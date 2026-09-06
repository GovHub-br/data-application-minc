-- Bronze SALIC — sac__vwprogramarouanettipologia.
-- Origem: salic_bronze.sac__vwprogramarouanettipologia, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 4 colunas: 1 tipadas, 2 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idverificacao") }} as idverificacao,
    {{ bronze_texto("idtipo") }} as idtipo,
    {{ bronze_texto("tipicidade") }} as tipicidade,
    _fatia
from {{ source("bronze_sac", "sac__vwprogramarouanettipologia") }}
