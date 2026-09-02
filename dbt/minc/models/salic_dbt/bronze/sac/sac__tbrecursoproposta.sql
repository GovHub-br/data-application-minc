-- Bronze SALIC — sac__tbrecursoproposta.
-- Origem: salic_bronze.sac__tbrecursoproposta, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 15 colunas: 9 tipadas, 5 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idrecursoproposta") }} as idrecursoproposta,
    {{ bronze_inteiro("idpreprojeto") }} as idpreprojeto,
    {{ bronze_timestamp("dtrecursoproponente") }} as dtrecursoproponente,
    {{ bronze_texto("dsrecursoproponente") }} as dsrecursoproponente,
    {{ bronze_inteiro("idproponente") }} as idproponente,
    {{ bronze_inteiro("idavaliadortecnico") }} as idavaliadortecnico,
    {{ bronze_timestamp("dtavaliacaotecnica") }} as dtavaliacaotecnica,
    {{ bronze_texto("dsavaliacaotecnica") }} as dsavaliacaotecnica,
    {{ bronze_texto("tprecurso") }} as tprecurso,
    {{ bronze_texto("tpsolicitacao") }} as tpsolicitacao,
    {{ bronze_texto("statendimento") }} as statendimento,
    {{ bronze_inteiro("idarquivo") }} as idarquivo,
    {{ bronze_booleano("stativo") }} as stativo,
    {{ bronze_booleano("strascunho") }} as strascunho,
    _fatia
from {{ source("bronze_sac", "sac__tbrecursoproposta") }}
