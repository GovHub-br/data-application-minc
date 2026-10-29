-- Silver SALIC — silver_sac_vw_captacao_ano_uf (ex-sac__vw_captacao_ano_uf).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_vw_captacao_ano_uf, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 5 colunas: 3 tipadas, 1 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("ano") }} as ano,
    {{ bronze_texto("uf") }} as uf,
    {{ bronze_inteiro("quantidade") }} as quantidade,
    {{ bronze_numerico("valor") }} as valor,
    _fatia
from {{ source("salic", "bronze_sac_vw_captacao_ano_uf") }}
