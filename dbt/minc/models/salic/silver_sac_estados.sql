-- Silver SALIC — silver_sac_estados (ex-sac__estados).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_estados, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 5 colunas: 2 tipadas, 2 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("id") }} as id,
    {{ bronze_texto("descricao") }} as descricao,
    {{ bronze_inteiro("fluxoid") }} as fluxoid,
    {{ bronze_texto("proximo") }} as proximo,
    _fatia
from {{ source("salic", "bronze_sac_estados") }}
