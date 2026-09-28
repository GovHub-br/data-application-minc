-- Bronze SALIC — sac__vwnotificacaoaoproponente.
-- Origem: salic_bronze.sac__vwnotificacaoaoproponente, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 15 colunas: 6 tipadas, 8 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_inteiro("idnotificacao") }} as idnotificacao,
    {{ bronze_inteiro("pronac") }} as pronac,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_texto("cdsituacao") }} as cdsituacao,
    {{ bronze_texto("dssituacao") }} as dssituacao,
    {{ bronze_timestamp("dtsituacao") }} as dtsituacao,
    {{ bronze_texto("tpnotificacao") }} as tpnotificacao,
    {{ bronze_texto("dsnotificacao") }} as dsnotificacao,
    {{ bronze_timestamp("dtenvionotificacao") }} as dtenvionotificacao,
    {{ bronze_timestamp("dtvisualizacaonotificacao") }} as dtvisualizacaonotificacao,
    {{ bronze_texto("stvisualizacao") }} as stvisualizacao,
    {{ bronze_texto("dsvisualizacao") }} as dsvisualizacao,
    {{ bronze_texto("stestado") }} as stestado,
    _fatia
from {{ source("bronze_sac", "sac__vwnotificacaoaoproponente") }}
