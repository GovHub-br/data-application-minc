-- Bronze SALIC — sac__vrenunciafiscalanoarea.
-- Origem: salic_bronze.sac__vrenunciafiscalanoarea, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 9 colunas: 8 tipadas, 0 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("ano") }} as ano,
    {{ bronze_numerico("area1") }} as area1,
    {{ bronze_numerico("area2") }} as area2,
    {{ bronze_numerico("area3") }} as area3,
    {{ bronze_numerico("area4") }} as area4,
    {{ bronze_numerico("area5") }} as area5,
    {{ bronze_numerico("area6") }} as area6,
    {{ bronze_numerico("area7") }} as area7,
    _fatia
from {{ source("bronze_sac", "sac__vrenunciafiscalanoarea") }}
