-- Bronze SALIC — sac__vcustodoprojeto.
-- Origem: salic_bronze.sac__vcustodoprojeto, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 5 colunas: 1 tipadas, 3 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("tipoprestacao") }} as tipoprestacao,
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_texto("sequencial") }} as sequencial,
    {{ bronze_numerico("custoprojeto") }} as custoprojeto,
    _fatia
from {{ source("bronze_sac", "sac__vcustodoprojeto") }}
