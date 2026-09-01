-- Bronze SALIC — sac__tbprogramarouanetavaliacao.
-- Origem: salic_bronze.sac__tbprogramarouanetavaliacao, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 8 colunas: 7 tipadas, 0 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idprogramarouanetavaliacao") }} as idprogramarouanetavaliacao,
    {{ bronze_inteiro("idprogramarouanet") }} as idprogramarouanet,
    {{ bronze_inteiro("idprogramarouanetpergunta") }} as idprogramarouanetpergunta,
    {{ bronze_inteiro("idpreprojeto") }} as idpreprojeto,
    {{ bronze_inteiro("idavaliador") }} as idavaliador,
    {{ bronze_inteiro("qtpontos") }} as qtpontos,
    {{ bronze_booleano("siavaliacao") }} as siavaliacao,
    _fatia
from {{ source("bronze_sac", "sac__tbprogramarouanetavaliacao") }}
