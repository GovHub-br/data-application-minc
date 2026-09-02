-- Bronze SALIC — sac__vcapmecaudioanoprojeto.
-- Origem: salic_bronze.sac__vcapmecaudioanoprojeto, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 5 colunas: 2 tipadas, 2 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("ano") }} as ano,
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_texto("sequencial") }} as sequencial,
    {{ bronze_numerico("apoioreal") }} as apoioreal,
    _fatia
from {{ source("bronze_sac", "sac__vcapmecaudioanoprojeto") }}
