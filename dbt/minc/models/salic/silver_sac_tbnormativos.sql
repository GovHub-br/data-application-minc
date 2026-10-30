-- Silver SALIC — silver_sac_tbnormativos (ex-sac__tbnormativos).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_tbnormativos, onde tudo chega como texto da
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
from {{ source("salic", "bronze_sac_tbnormativos") }}
