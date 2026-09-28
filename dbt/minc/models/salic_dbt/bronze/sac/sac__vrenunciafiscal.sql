-- Bronze SALIC — sac__vrenunciafiscal.
-- Origem: salic_bronze.sac__vrenunciafiscal, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 6 colunas: 5 tipadas, 0 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("ano") }} as ano,
    {{ bronze_numerico("trim1") }} as trim1,
    {{ bronze_numerico("trim2") }} as trim2,
    {{ bronze_numerico("trim3") }} as trim3,
    {{ bronze_numerico("trim4") }} as trim4,
    _fatia
from {{ source("bronze_sac", "sac__vrenunciafiscal") }}
