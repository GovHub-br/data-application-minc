-- Silver SALIC — silver_sac_tborgaofiscalizador (ex-sac__tborgaofiscalizador).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_tborgaofiscalizador, onde tudo chega como texto da
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
from {{ source("salic", "bronze_sac_tborgaofiscalizador") }}
