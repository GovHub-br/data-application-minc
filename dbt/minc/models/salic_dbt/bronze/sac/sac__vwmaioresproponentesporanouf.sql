-- Bronze SALIC — sac__vwmaioresproponentesporanouf.
-- Origem: salic_bronze.sac__vwmaioresproponentesporanouf, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 7 colunas: 3 tipadas, 3 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("uf") }} as uf,
    {{ bronze_inteiro("ano") }} as ano,
    {{ bronze_texto("cgccpf") }} as cgccpf,
    {{ bronze_texto("proponente") }} as proponente,
    {{ bronze_numerico("vlcaptado") }} as vlcaptado,
    {{ bronze_inteiro("ranking") }} as ranking,
    _fatia
from {{ source("bronze_sac", "sac__vwmaioresproponentesporanouf") }}
