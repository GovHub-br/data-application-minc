-- Bronze SALIC — tabelas__temp_tbl.
-- Origem: salic_bronze.tabelas__temp_tbl, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 2 colunas: 0 tipadas, 1 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select {{ bronze_texto("tmp_texto") }} as tmp_texto, _fatia
from {{ source("bronze_tabelas", "tabelas__temp_tbl") }}
