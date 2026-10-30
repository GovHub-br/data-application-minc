-- Silver SALIC — silver_sac_tbmensagemdispositivomovel (ex-sac__tbmensagemdispositivomovel).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_tbmensagemdispositivomovel, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 5 colunas: 4 tipadas, 0 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idmensagemdispositivomovel") }} as idmensagemdispositivomovel,
    {{ bronze_inteiro("idmensagem") }} as idmensagem,
    {{ bronze_inteiro("iddispositivomovel") }} as iddispositivomovel,
    {{ bronze_timestamp("dtexclusao") }} as dtexclusao,
    _fatia
from {{ source("salic", "bronze_sac_tbmensagemdispositivomovel") }}
