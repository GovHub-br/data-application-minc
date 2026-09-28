-- Bronze SALIC — sac__vfnaprocapprojmec.
-- Origem: salic_bronze.sac__vfnaprocapprojmec, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 5 colunas: 1 tipadas, 3 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_inteiro("sequencial") }} as sequencial,
    {{ bronze_texto("apromec") }} as apromec,
    {{ bronze_texto("capmec") }} as capmec,
    _fatia
from {{ source("bronze_sac", "sac__vfnaprocapprojmec") }}
