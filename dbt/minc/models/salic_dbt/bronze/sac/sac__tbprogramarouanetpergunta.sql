-- Bronze SALIC — sac__tbprogramarouanetpergunta.
-- Origem: salic_bronze.sac__tbprogramarouanetpergunta, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 8 colunas: 6 tipadas, 1 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idprogramarouanetpergunta") }} as idprogramarouanetpergunta,
    {{ bronze_inteiro("idprogramarouanet") }} as idprogramarouanet,
    {{ bronze_texto("dspergunta") }} as dspergunta,
    {{ bronze_inteiro("nrfaixainicio") }} as nrfaixainicio,
    {{ bronze_inteiro("nrfaixaincremento") }} as nrfaixaincremento,
    {{ bronze_inteiro("nrfaixafinal") }} as nrfaixafinal,
    {{ bronze_inteiro("nrordem") }} as nrordem,
    _fatia
from {{ source("bronze_sac", "sac__tbprogramarouanetpergunta") }}
