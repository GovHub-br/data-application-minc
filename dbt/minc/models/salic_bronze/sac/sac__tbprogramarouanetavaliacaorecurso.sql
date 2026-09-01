-- Bronze SALIC — sac__tbprogramarouanetavaliacaorecurso.
-- Origem: salic_bronze.sac__tbprogramarouanetavaliacaorecurso, onde tudo chega como
-- texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 9 colunas: 8 tipadas, 0 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idprogramarouanetavaliacaorecurso") }}
    as idprogramarouanetavaliacaorecurso,
    {{ bronze_inteiro("idprogramarouanet") }} as idprogramarouanet,
    {{ bronze_inteiro("idprogramarouanetpergunta") }} as idprogramarouanetpergunta,
    {{ bronze_inteiro("idpreprojeto") }} as idpreprojeto,
    {{ bronze_inteiro("idavaliador") }} as idavaliador,
    {{ bronze_numerico("qtmediapontos") }} as qtmediapontos,
    {{ bronze_numerico("qtpontosrecurso") }} as qtpontosrecurso,
    {{ bronze_booleano("siavaliacaorecurso") }} as siavaliacaorecurso,
    _fatia
from {{ source("bronze_sac", "sac__tbprogramarouanetavaliacaorecurso") }}
