-- Bronze SALIC — sac__dba_qtd_proponentes_inabilitados.
-- Origem: salic_bronze.sac__dba_qtd_proponentes_inabilitados, onde tudo chega como
-- texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 2 colunas: 0 tipadas, 1 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select {{ bronze_texto("qtd") }} as qtd, _fatia
from {{ source("bronze_sac", "sac__dba_qtd_proponentes_inabilitados") }}
