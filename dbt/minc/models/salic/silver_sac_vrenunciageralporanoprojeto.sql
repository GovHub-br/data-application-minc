-- Silver SALIC — silver_sac_vrenunciageralporanoprojeto
-- (ex-sac__vrenunciageralporanoprojeto).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_vrenunciageralporanoprojeto, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 7 colunas: 3 tipadas, 3 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("ano") }} as ano,
    {{ bronze_texto("lei") }} as lei,
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_texto("sequencial") }} as sequencial,
    {{ bronze_numerico("captado") }} as captado,
    {{ bronze_numerico("renuncia") }} as renuncia,
    _fatia
from {{ source("salic", "bronze_sac_vrenunciageralporanoprojeto") }}
