-- Bronze SALIC — sac__vsaldoaprovadoporano.
-- Origem: salic_bronze.sac__vsaldoaprovadoporano, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 15 colunas: 7 tipadas, 7 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_inteiro("sequencial") }} as sequencial,
    {{ bronze_inteiro("ano") }} as ano,
    {{ bronze_timestamp("dtaprovacao") }} as dtaprovacao,
    {{ bronze_numerico("apromec") }} as apromec,
    {{ bronze_texto("aproart1") }} as aproart1,
    {{ bronze_texto("aproart3") }} as aproart3,
    {{ bronze_numerico("aprocusteio") }} as aprocusteio,
    {{ bronze_numerico("compmec") }} as compmec,
    {{ bronze_texto("compart1") }} as compart1,
    {{ bronze_texto("compart3") }} as compart3,
    {{ bronze_numerico("redumec") }} as redumec,
    {{ bronze_texto("reduart1") }} as reduart1,
    {{ bronze_texto("reduart3") }} as reduart3,
    _fatia
from {{ source("bronze_sac", "sac__vsaldoaprovadoporano") }}
