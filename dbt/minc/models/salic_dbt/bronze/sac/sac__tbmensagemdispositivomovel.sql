-- Bronze SALIC — sac__tbmensagemdispositivomovel.
-- Origem: salic_bronze.sac__tbmensagemdispositivomovel, onde tudo chega como texto da
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
from {{ source("bronze_sac", "sac__tbmensagemdispositivomovel") }}
