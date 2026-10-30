-- Silver SALIC — silver_sac_tbmovimentacao (ex-sac__tbmovimentacao).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_tbmovimentacao, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 7 colunas: 6 tipadas, 0 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idmovimentacao") }} as idmovimentacao,
    {{ bronze_inteiro("idprojeto") }} as idprojeto,
    {{ bronze_inteiro("movimentacao") }} as movimentacao,
    {{ bronze_timestamp("dtmovimentacao") }} as dtmovimentacao,
    {{ bronze_booleano("stestado") }} as stestado,
    {{ bronze_inteiro("usuario") }} as usuario,
    _fatia
from {{ source("salic", "bronze_sac_tbmovimentacao") }}
