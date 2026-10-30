-- Silver SALIC — silver_sac_dbf_maio (ex-sac__dbf_maio).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_dbf_maio, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 2 colunas: 0 tipadas, 1 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select {{ bronze_texto("informacao") }} as informacao, _fatia
from {{ source("salic", "bronze_sac_dbf_maio") }}
