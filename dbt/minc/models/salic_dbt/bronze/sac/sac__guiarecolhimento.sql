-- Bronze SALIC — sac__guiarecolhimento.
-- Origem: salic_bronze.sac__guiarecolhimento, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 8 colunas: 6 tipadas, 1 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("numeroguia") }} as numeroguia,
    {{ bronze_texto("cgccpf") }} as cgccpf,
    {{ bronze_timestamp("dtrecolhimento") }} as dtrecolhimento,
    {{ bronze_numerico("valor") }} as valor,
    {{ bronze_inteiro("statusguia") }} as statusguia,
    {{ bronze_timestamp("dtfunarte") }} as dtfunarte,
    {{ bronze_inteiro("logon") }} as logon,
    _fatia
from {{ source("bronze_sac", "sac__guiarecolhimento") }}
