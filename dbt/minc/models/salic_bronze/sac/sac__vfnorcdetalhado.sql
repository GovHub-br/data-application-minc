-- Bronze SALIC — sac__vfnorcdetalhado.
-- Origem: salic_bronze.sac__vfnorcdetalhado, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 9 colunas: 1 tipadas, 7 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_inteiro("sequencial") }} as sequencial,
    {{ bronze_texto("promec") }} as promec,
    {{ bronze_texto("commec") }} as commec,
    {{ bronze_texto("proart1") }} as proart1,
    {{ bronze_texto("comart1") }} as comart1,
    {{ bronze_texto("proart3") }} as proart3,
    {{ bronze_texto("comart3") }} as comart3,
    _fatia
from {{ source("bronze_sac", "sac__vfnorcdetalhado") }}
