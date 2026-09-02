-- Bronze SALIC — sac__vincentivadoresma.
-- Origem: salic_bronze.sac__vincentivadoresma, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 3 colunas: 1 tipadas, 1 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select {{ bronze_inteiro("ano") }} as ano, {{ bronze_texto("cgccpf") }} as cgccpf, _fatia
from {{ source("bronze_sac", "sac__vincentivadoresma") }}
