-- Bronze SALIC — sac__vrenunciaporano.
-- Origem: salic_bronze.sac__vrenunciaporano, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 9 colunas: 4 tipadas, 4 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("ano") }} as ano,
    {{ bronze_texto("cgccpf") }} as cgccpf,
    {{ bronze_texto("mp") }} as mp,
    {{ bronze_inteiro("anoprojeto") }} as anoprojeto,
    {{ bronze_texto("sequencial") }} as sequencial,
    {{ bronze_texto("tipoapoio") }} as tipoapoio,
    {{ bronze_numerico("apoioufir") }} as apoioufir,
    {{ bronze_numerico("apoioreal") }} as apoioreal,
    _fatia
from {{ source("bronze_sac", "sac__vrenunciaporano") }}
