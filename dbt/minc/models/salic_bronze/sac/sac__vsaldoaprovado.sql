-- Bronze SALIC — sac__vsaldoaprovado.
-- Origem: salic_bronze.sac__vsaldoaprovado, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 19 colunas: 10 tipadas, 8 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_inteiro("sequencial") }} as sequencial,
    {{ bronze_timestamp("dtaprovacao") }} as dtaprovacao,
    {{ bronze_timestamp("dtiniciocaptacao") }} as dtiniciocaptacao,
    {{ bronze_timestamp("dtfimcaptacao") }} as dtfimcaptacao,
    {{ bronze_numerico("apromec") }} as apromec,
    {{ bronze_texto("aproart1") }} as aproart1,
    {{ bronze_numerico("aproart3") }} as aproart3,
    {{ bronze_numerico("aproconv") }} as aproconv,
    {{ bronze_numerico("aprocontra") }} as aprocontra,
    {{ bronze_numerico("compmec") }} as compmec,
    {{ bronze_texto("compart1") }} as compart1,
    {{ bronze_texto("compart3") }} as compart3,
    {{ bronze_texto("compconv") }} as compconv,
    {{ bronze_numerico("redumec") }} as redumec,
    {{ bronze_texto("reduart1") }} as reduart1,
    {{ bronze_texto("reduart3") }} as reduart3,
    {{ bronze_texto("reduconv") }} as reduconv,
    _fatia
from {{ source("bronze_sac", "sac__vsaldoaprovado") }}
