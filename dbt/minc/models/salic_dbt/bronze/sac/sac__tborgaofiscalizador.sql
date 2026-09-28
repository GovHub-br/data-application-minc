-- Bronze SALIC — sac__tborgaofiscalizador.
-- Origem: salic_bronze.sac__tborgaofiscalizador, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 9 colunas: 7 tipadas, 1 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idorgaofiscalizador") }} as idorgaofiscalizador,
    {{ bronze_inteiro("idorgao") }} as idorgao,
    {{ bronze_inteiro("idfiscalizacao") }} as idfiscalizacao,
    {{ bronze_texto("dsobservacao") }} as dsobservacao,
    {{ bronze_timestamp("dtrecebimentoresposta") }} as dtrecebimentoresposta,
    {{ bronze_timestamp("dtconfirmacaofiscalizacao") }} as dtconfirmacaofiscalizacao,
    {{ bronze_inteiro("idresponsavelconfirmacao") }} as idresponsavelconfirmacao,
    {{ bronze_inteiro("idparecerista") }} as idparecerista,
    _fatia
from {{ source("bronze_sac", "sac__tborgaofiscalizador") }}
