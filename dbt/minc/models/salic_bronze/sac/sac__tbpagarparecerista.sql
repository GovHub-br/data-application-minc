-- Bronze SALIC — sac__tbpagarparecerista.
-- Origem: salic_bronze.sac__tbpagarparecerista, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 13 colunas: 11 tipadas, 1 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idpagarparecerista") }} as idpagarparecerista,
    {{ bronze_inteiro("idnrreuniao") }} as idnrreuniao,
    {{ bronze_inteiro("idparecerista") }} as idparecerista,
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_inteiro("idproduto") }} as idproduto,
    {{ bronze_inteiro("idunidadeanalise") }} as idunidadeanalise,
    {{ bronze_inteiro("idgerarpagamentoparecerista") }} as idgerarpagamentoparecerista,
    {{ bronze_numerico("vlpagamento") }} as vlpagamento,
    {{ bronze_timestamp("dtdistribuicao") }} as dtdistribuicao,
    {{ bronze_timestamp("dthomologacao") }} as dthomologacao,
    {{ bronze_inteiro("idpareceristaempenho") }} as idpareceristaempenho,
    {{ bronze_texto("nrempenho") }} as nrempenho,
    _fatia
from {{ source("bronze_sac", "sac__tbpagarparecerista") }}
