-- Silver SALIC — silver_sac_vportariaaprovado (ex-sac__vportariaaprovado).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_vportariaaprovado, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 4 colunas: 2 tipadas, 1 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_inteiro("sequencial") }} as sequencial,
    {{ bronze_timestamp("dtaprovacao") }} as dtaprovacao,
    _fatia
from {{ source("salic", "bronze_sac_vportariaaprovado") }}
