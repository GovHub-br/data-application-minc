-- Bronze SALIC — sac__tbplanilhaunidade.
-- Origem: salic_bronze.sac__tbplanilhaunidade, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 4 colunas: 1 tipadas, 2 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idunidade") }} as idunidade,
    {{ bronze_texto("sigla") }} as sigla,
    {{ bronze_texto("descricao") }} as descricao,
    _fatia
from {{ source("bronze_sac", "sac__tbplanilhaunidade") }}
