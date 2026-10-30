-- Silver SALIC — silver_sac_tbprogramarouanetrecurso (ex-sac__tbprogramarouanetrecurso).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_tbprogramarouanetrecurso, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 11 colunas: 7 tipadas, 3 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idprogramarouanetrecurso") }} as idprogramarouanetrecurso,
    {{ bronze_inteiro("idprogramarouanet") }} as idprogramarouanet,
    {{ bronze_inteiro("idpreprojeto") }} as idpreprojeto,
    {{ bronze_timestamp("dtrecurso") }} as dtrecurso,
    {{ bronze_texto("dsrecurso") }} as dsrecurso,
    {{ bronze_inteiro("simanifestacao") }} as simanifestacao,
    {{ bronze_timestamp("dtmanifestacao") }} as dtmanifestacao,
    {{ bronze_texto("dsmanifestacao") }} as dsmanifestacao,
    {{ bronze_texto("sirecurso") }} as sirecurso,
    {{ bronze_booleano("siestado") }} as siestado,
    _fatia
from {{ source("salic", "bronze_sac_tbprogramarouanetrecurso") }}
