-- Bronze SALIC — sac__tbplanodivulgacao.
-- Origem: salic_bronze.sac__tbplanodivulgacao, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 10 colunas: 5 tipadas, 4 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idplanodivulgacao") }} as idplanodivulgacao,
    {{ bronze_inteiro("idreadequacao") }} as idreadequacao,
    {{ bronze_inteiro("idpeca") }} as idpeca,
    {{ bronze_inteiro("idveiculo") }} as idveiculo,
    {{ bronze_texto("tpsolicitacao") }} as tpsolicitacao,
    {{ bronze_texto("tpanalisetecnica") }} as tpanalisetecnica,
    {{ bronze_texto("tpanalisecomissao") }} as tpanalisecomissao,
    {{ bronze_texto("stativo") }} as stativo,
    {{ bronze_inteiro("idpronac") }} as idpronac,
    _fatia
from {{ source("bronze_sac", "sac__tbplanodivulgacao") }}
