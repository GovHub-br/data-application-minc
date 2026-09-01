-- Bronze SALIC — sac__vconvenioanoarea.
-- Origem: salic_bronze.sac__vconvenioanoarea, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 5 colunas: 4 tipadas, 0 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("ano") }} as ano,
    {{ bronze_inteiro("area") }} as area,
    {{ bronze_inteiro("segmento") }} as segmento,
    {{ bronze_numerico("valorconveniol") }} as valorconveniol,
    _fatia
from {{ source("bronze_sac", "sac__vconvenioanoarea") }}
