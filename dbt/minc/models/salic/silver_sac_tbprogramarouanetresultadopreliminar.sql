-- Silver SALIC — silver_sac_tbprogramarouanetresultadopreliminar
-- (ex-sac__tbprogramarouanetresultadopreliminar).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_tbprogramarouanetresultadopreliminar, onde tudo chega como
-- texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 6 colunas: 4 tipadas, 1 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idprogramarouanetresultadopreliminar") }}
    as idprogramarouanetresultadopreliminar,
    {{ bronze_inteiro("idprogramarouanet") }} as idprogramarouanet,
    {{ bronze_inteiro("idpreprojeto") }} as idpreprojeto,
    {{ bronze_numerico("nrpontuacao") }} as nrpontuacao,
    {{ bronze_texto("siresultadopreliminar") }} as siresultadopreliminar,
    _fatia
from {{ source("salic", "bronze_sac_tbprogramarouanetresultadopreliminar") }}
