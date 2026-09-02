-- Bronze SALIC — sac__vproponentecommaisdecincoproj.
-- Origem: salic_bronze.sac__vproponentecommaisdecincoproj, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 4 colunas: 1 tipadas, 2 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("cgccpf") }} as cgccpf,
    {{ bronze_texto("proponente") }} as proponente,
    {{ bronze_inteiro("qtde") }} as qtde,
    _fatia
from {{ source("bronze_sac", "sac__vproponentecommaisdecincoproj") }}
