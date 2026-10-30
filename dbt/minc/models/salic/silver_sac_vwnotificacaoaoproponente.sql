-- Silver SALIC — silver_sac_vwnotificacaoaoproponente
-- (ex-sac__vwnotificacaoaoproponente).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_vwnotificacaoaoproponente, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 15 colunas: 5 tipadas, 9 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_inteiro("idnotificacao") }} as idnotificacao,
    {{ bronze_texto("pronac") }} as pronac,
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
from {{ source("salic", "bronze_sac_vwnotificacaoaoproponente") }}
