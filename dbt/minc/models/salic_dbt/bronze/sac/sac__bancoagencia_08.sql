-- Bronze SALIC — sac__bancoagencia_08.
-- Origem: salic_bronze.sac__bancoagencia_08, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 2 colunas: 0 tipadas, 1 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select {{ bronze_texto("banco") }} as banco, _fatia
from {{ source("bronze_sac", "sac__bancoagencia_08") }}
