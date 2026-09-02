-- Bronze SALIC — sac__vprestacao.
-- Origem: salic_bronze.sac__vprestacao, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 12 colunas: 9 tipadas, 2 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("area") }} as area,
    {{ bronze_inteiro("segmento") }} as segmento,
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_inteiro("sequencial") }} as sequencial,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_timestamp("dtiniciorealizacao") }} as dtiniciorealizacao,
    {{ bronze_timestamp("dtfinalrealizacao") }} as dtfinalrealizacao,
    {{ bronze_numerico("valorexecutado") }} as valorexecutado,
    {{ bronze_numerico("aplicacaofinanceira") }} as aplicacaofinanceira,
    {{ bronze_numerico("outrasfontes") }} as outrasfontes,
    {{ bronze_numerico("saldorecolhido") }} as saldorecolhido,
    _fatia
from {{ source("bronze_sac", "sac__vprestacao") }}
