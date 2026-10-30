-- Silver SALIC — silver_sac_vguianaovinculada (ex-sac__vguianaovinculada).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_vguianaovinculada, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 5 colunas: 3 tipadas, 1 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("numeroguia") }} as numeroguia,
    {{ bronze_texto("cgccpf") }} as cgccpf,
    {{ bronze_timestamp("dtrecolhimento") }} as dtrecolhimento,
    {{ bronze_numerico("valor") }} as valor,
    _fatia
from {{ source("salic", "bronze_sac_vguianaovinculada") }}
