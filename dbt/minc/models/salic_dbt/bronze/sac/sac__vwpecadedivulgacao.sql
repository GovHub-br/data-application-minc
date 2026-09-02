-- Bronze SALIC — sac__vwpecadedivulgacao.
-- Origem: salic_bronze.sac__vwpecadedivulgacao, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 5 colunas: 1 tipadas, 3 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idverificacao") }} as idverificacao,
    {{ bronze_texto("idtipo") }} as idtipo,
    {{ bronze_texto("pecadedivulgacao") }} as pecadedivulgacao,
    {{ bronze_texto("stestado") }} as stestado,
    _fatia
from {{ source("bronze_sac", "sac__vwpecadedivulgacao") }}
