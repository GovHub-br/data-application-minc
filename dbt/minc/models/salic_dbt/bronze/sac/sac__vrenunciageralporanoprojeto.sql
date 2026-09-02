-- Bronze SALIC — sac__vrenunciageralporanoprojeto.
-- Origem: salic_bronze.sac__vrenunciageralporanoprojeto, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 7 colunas: 4 tipadas, 2 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("ano") }} as ano,
    {{ bronze_texto("lei") }} as lei,
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_inteiro("sequencial") }} as sequencial,
    {{ bronze_numerico("captado") }} as captado,
    {{ bronze_numerico("renuncia") }} as renuncia,
    _fatia
from {{ source("bronze_sac", "sac__vrenunciageralporanoprojeto") }}
