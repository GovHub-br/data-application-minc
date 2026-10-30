-- Silver SALIC — silver_sac_tbdistribuirreadequacao (ex-sac__tbdistribuirreadequacao).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_tbdistribuirreadequacao, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 12 colunas: 10 tipadas, 1 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("iddistribuirreadequacao") }} as iddistribuirreadequacao,
    {{ bronze_inteiro("idreadequacao") }} as idreadequacao,
    {{ bronze_inteiro("idunidade") }} as idunidade,
    {{ bronze_timestamp("dtencaminhamento") }} as dtencaminhamento,
    {{ bronze_inteiro("idavaliador") }} as idavaliador,
    {{ bronze_timestamp("dtenvioavaliador") }} as dtenvioavaliador,
    {{ bronze_texto("dsorientacao") }} as dsorientacao,
    {{ bronze_timestamp("dtretornoavaliador") }} as dtretornoavaliador,
    {{ bronze_booleano("stvalidacaocoordenador") }} as stvalidacaocoordenador,
    {{ bronze_timestamp("dtvalidacaocoordenador") }} as dtvalidacaocoordenador,
    {{ bronze_inteiro("idcoordenador") }} as idcoordenador,
    _fatia
from {{ source("salic", "bronze_sac_tbdistribuirreadequacao") }}
