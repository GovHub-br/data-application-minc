-- Silver SALIC — silver_sac_vfnorcdetalhado (ex-sac__vfnorcdetalhado).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_vfnorcdetalhado, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 9 colunas: 0 tipadas, 8 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_texto("sequencial") }} as sequencial,
    {{ bronze_texto("promec") }} as promec,
    {{ bronze_texto("commec") }} as commec,
    {{ bronze_texto("proart1") }} as proart1,
    {{ bronze_texto("comart1") }} as comart1,
    {{ bronze_texto("proart3") }} as proart3,
    {{ bronze_texto("comart3") }} as comart3,
    _fatia
from {{ source("salic", "bronze_sac_vfnorcdetalhado") }}
