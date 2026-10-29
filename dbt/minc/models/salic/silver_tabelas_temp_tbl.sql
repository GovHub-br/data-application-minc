-- Silver SALIC — silver_tabelas_temp_tbl (ex-tabelas__temp_tbl).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_tabelas_temp_tbl, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 2 colunas: 0 tipadas, 1 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select {{ bronze_texto("tmp_texto") }} as tmp_texto, _fatia
from {{ source("salic", "bronze_tabelas_temp_tbl") }}
