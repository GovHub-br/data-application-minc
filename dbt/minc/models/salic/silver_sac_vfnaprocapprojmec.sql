-- Silver SALIC — silver_sac_vfnaprocapprojmec (ex-sac__vfnaprocapprojmec).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_vfnaprocapprojmec, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 5 colunas: 0 tipadas, 4 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_texto("sequencial") }} as sequencial,
    {{ bronze_texto("apromec") }} as apromec,
    {{ bronze_texto("capmec") }} as capmec,
    _fatia
from {{ source("salic", "bronze_sac_vfnaprocapprojmec") }}
