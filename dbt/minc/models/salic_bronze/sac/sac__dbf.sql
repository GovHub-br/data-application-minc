-- Bronze SALIC — sac__dbf.
-- Origem: salic_bronze.sac__dbf, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 5 colunas: 0 tipadas, 4 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("tiporegistro") }} as tiporegistro,
    {{ bronze_texto("ano") }} as ano,
    {{ bronze_texto("sequencial") }} as sequencial,
    {{ bronze_texto("informacao") }} as informacao,
    _fatia
from {{ source("bronze_sac", "sac__dbf") }}
