-- Silver SALIC — silver_sac_editalparcelas (ex-sac__editalparcelas).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_editalparcelas, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 9 colunas: 8 tipadas, 0 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idedital") }} as idedital,
    {{ bronze_inteiro("nrparcela") }} as nrparcela,
    {{ bronze_inteiro("faixa") }} as faixa,
    {{ bronze_inteiro("diasparaliberar") }} as diasparaliberar,
    {{ bronze_numerico("valor") }} as valor,
    {{ bronze_booleano("exigepc") }} as exigepc,
    {{ bronze_inteiro("parcela") }} as parcela,
    {{ bronze_inteiro("logon") }} as logon,
    _fatia
from {{ source("salic", "bronze_sac_editalparcelas") }}
