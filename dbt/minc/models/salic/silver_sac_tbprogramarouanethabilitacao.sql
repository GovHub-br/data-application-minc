-- Silver SALIC — silver_sac_tbprogramarouanethabilitacao
-- (ex-sac__tbprogramarouanethabilitacao).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_tbprogramarouanethabilitacao, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 8 colunas: 6 tipadas, 1 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idprogramarouanethabilitacao") }} as idprogramarouanethabilitacao,
    {{ bronze_inteiro("idprogramarouanet") }} as idprogramarouanet,
    {{ bronze_inteiro("idpreprojeto") }} as idpreprojeto,
    {{ bronze_timestamp("dthabilitacao") }} as dthabilitacao,
    {{ bronze_texto("dshabilitacao") }} as dshabilitacao,
    {{ bronze_booleano("sihabilitacao") }} as sihabilitacao,
    {{ bronze_inteiro("idtecnico") }} as idtecnico,
    _fatia
from {{ source("salic", "bronze_sac_tbprogramarouanethabilitacao") }}
