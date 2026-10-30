-- Silver SALIC — silver_sac_vportarianormal (ex-sac__vportarianormal).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_vportarianormal, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 5 colunas: 1 tipadas, 3 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_texto("sequencial") }} as sequencial,
    {{ bronze_texto("tipoaprovacao") }} as tipoaprovacao,
    {{ bronze_timestamp("dtaprovacao") }} as dtaprovacao,
    _fatia
from {{ source("salic", "bronze_sac_vportarianormal") }}
