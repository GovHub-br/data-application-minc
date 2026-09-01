-- Bronze SALIC — sac__tbpagarpareceristareadequacao.
-- Origem: salic_bronze.sac__tbpagarpareceristareadequacao, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 12 colunas: 10 tipadas, 1 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idpagarreadequacao") }} as idpagarreadequacao,
    {{ bronze_inteiro("idparecerista") }} as idparecerista,
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_timestamp("dthomologacao") }} as dthomologacao,
    {{ bronze_inteiro("idunidade") }} as idunidade,
    {{ bronze_inteiro("idgerarpagamentoparecerista") }} as idgerarpagamentoparecerista,
    {{ bronze_numerico("vlpagamento") }} as vlpagamento,
    {{ bronze_inteiro("idusuario") }} as idusuario,
    {{ bronze_timestamp("dtdistribuicao") }} as dtdistribuicao,
    {{ bronze_inteiro("idpareceristaempenho") }} as idpareceristaempenho,
    {{ bronze_texto("nrempenho") }} as nrempenho,
    _fatia
from {{ source("bronze_sac", "sac__tbpagarpareceristareadequacao") }}
