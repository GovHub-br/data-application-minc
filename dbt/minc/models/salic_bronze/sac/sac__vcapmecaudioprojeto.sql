-- Bronze SALIC — sac__vcapmecaudioprojeto.
-- Origem: salic_bronze.sac__vcapmecaudioprojeto, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 4 colunas: 1 tipadas, 2 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_texto("sequencial") }} as sequencial,
    {{ bronze_numerico("apoioreal") }} as apoioreal,
    _fatia
from {{ source("bronze_sac", "sac__vcapmecaudioprojeto") }}
