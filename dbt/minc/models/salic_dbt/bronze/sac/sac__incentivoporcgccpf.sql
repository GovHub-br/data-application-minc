-- Bronze SALIC — sac__incentivoporcgccpf.
-- Origem: salic_bronze.sac__incentivoporcgccpf, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 3 colunas: 1 tipadas, 1 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("cgccpf") }} as cgccpf,
    {{ bronze_numerico("apoioreal") }} as apoioreal,
    _fatia
from {{ source("bronze_sac", "sac__incentivoporcgccpf") }}
