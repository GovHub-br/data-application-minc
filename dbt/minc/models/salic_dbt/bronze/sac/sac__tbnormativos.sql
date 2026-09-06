-- Bronze SALIC — sac__tbnormativos.
-- Origem: salic_bronze.sac__tbnormativos, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 8 colunas: 4 tipadas, 3 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idnormativo") }} as idnormativo,
    {{ bronze_texto("nmnormativo") }} as nmnormativo,
    {{ bronze_texto("dsjustificativa") }} as dsjustificativa,
    {{ bronze_texto("dscomentario") }} as dscomentario,
    {{ bronze_timestamp("dtpublicacao") }} as dtpublicacao,
    {{ bronze_timestamp("dtrevogacao") }} as dtrevogacao,
    {{ bronze_booleano("stestado") }} as stestado,
    _fatia
from {{ source("bronze_sac", "sac__tbnormativos") }}
