-- Silver SALIC — silver_sac_tbhistoricoalteracaodocumento (ex-sac__tbhistoricoalteracaodocumento).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_tbhistoricoalteracaodocumento, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 5 colunas: 4 tipadas, 0 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idhistoricoalteracaodocumento") }}
    as idhistoricoalteracaodocumento,
    {{ bronze_inteiro("iddocumento") }} as iddocumento,
    {{ bronze_inteiro("idhistoricoalteracaoprojeto") }} as idhistoricoalteracaoprojeto,
    {{ bronze_inteiro("iddocumentosexigidos") }} as iddocumentosexigidos,
    _fatia
from {{ source("salic", "bronze_sac_tbhistoricoalteracaodocumento") }}
