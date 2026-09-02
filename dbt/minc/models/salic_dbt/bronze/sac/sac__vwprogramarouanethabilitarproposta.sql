-- Bronze SALIC — sac__vwprogramarouanethabilitarproposta.
-- Origem: salic_bronze.sac__vwprogramarouanethabilitarproposta, onde tudo chega como
-- texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 25 colunas: 8 tipadas, 16 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idprogramarouanethabilitacao") }} as idprogramarouanethabilitacao,
    {{ bronze_texto("idprogramarouanet") }} as idprogramarouanet,
    {{ bronze_inteiro("idpreprojeto") }} as idpreprojeto,
    {{ bronze_texto("dthabilitacao") }} as dthabilitacao,
    {{ bronze_texto("dshabilitacao") }} as dshabilitacao,
    {{ bronze_texto("sihabilitacao") }} as sihabilitacao,
    {{ bronze_texto("idtecnico") }} as idtecnico,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_inteiro("idagente") }} as idagente,
    {{ bronze_timestamp("dtiniciodeexecucao") }} as dtiniciodeexecucao,
    {{ bronze_timestamp("dtfinaldeexecucao") }} as dtfinaldeexecucao,
    {{ bronze_texto("tptipologia") }} as tptipologia,
    {{ bronze_texto("dtabertura") }} as dtabertura,
    {{ bronze_texto("dtfechamento") }} as dtfechamento,
    {{ bronze_texto("dtresultado") }} as dtresultado,
    {{ bronze_texto("qtavaliadores") }} as qtavaliadores,
    {{ bronze_texto("qtpropostas") }} as qtpropostas,
    {{ bronze_texto("tppessoa") }} as tppessoa,
    {{ bronze_texto("vlmaximoproposta") }} as vlmaximoproposta,
    {{ bronze_inteiro("area") }} as area,
    {{ bronze_texto("segmento") }} as segmento,
    {{ bronze_inteiro("uf") }} as uf,
    {{ bronze_inteiro("cidade") }} as cidade,
    {{ bronze_texto("proponente") }} as proponente,
    _fatia
from {{ source("bronze_sac", "sac__vwprogramarouanethabilitarproposta") }}
