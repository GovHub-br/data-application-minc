-- Bronze SALIC — sac__vcartaaudio.
-- Origem: salic_bronze.sac__vcartaaudio, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 3 colunas: 0 tipadas, 2 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_texto("sequencial") }} as sequencial,
    _fatia
from {{ source("bronze_sac", "sac__vcartaaudio") }}
