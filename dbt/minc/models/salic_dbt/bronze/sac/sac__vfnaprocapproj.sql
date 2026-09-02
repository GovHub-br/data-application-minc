-- Bronze SALIC — sac__vfnaprocapproj.
-- Origem: salic_bronze.sac__vfnaprocapproj, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 13 colunas: 6 tipadas, 6 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_inteiro("sequencial") }} as sequencial,
    {{ bronze_numerico("apromec") }} as apromec,
    {{ bronze_numerico("capmec") }} as capmec,
    {{ bronze_texto("aproart1") }} as aproart1,
    {{ bronze_texto("capart1") }} as capart1,
    {{ bronze_numerico("aproart3") }} as aproart3,
    {{ bronze_texto("capart3") }} as capart3,
    {{ bronze_numerico("aproconv") }} as aproconv,
    {{ bronze_texto("capconv") }} as capconv,
    {{ bronze_numerico("aprocontra") }} as aprocontra,
    {{ bronze_texto("capcontra") }} as capcontra,
    _fatia
from {{ source("bronze_sac", "sac__vfnaprocapproj") }}
