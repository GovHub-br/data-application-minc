-- Bronze SALIC — sac__tbprogramarouanethabilitacao.
-- Origem: salic_bronze.sac__tbprogramarouanethabilitacao, onde tudo chega como texto da
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
from {{ source("bronze_sac", "sac__tbprogramarouanethabilitacao") }}
