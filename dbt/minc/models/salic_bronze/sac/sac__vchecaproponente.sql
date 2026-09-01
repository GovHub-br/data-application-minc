-- Bronze SALIC — sac__vchecaproponente.
-- Origem: salic_bronze.sac__vchecaproponente, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 6 colunas: 4 tipadas, 1 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("cgccpf") }} as cgccpf,
    {{ bronze_inteiro("quant") }} as quant,
    {{ bronze_numerico("solicitado") }} as solicitado,
    {{ bronze_numerico("aprovado") }} as aprovado,
    {{ bronze_numerico("captado") }} as captado,
    _fatia
from {{ source("bronze_sac", "sac__vchecaproponente") }}
