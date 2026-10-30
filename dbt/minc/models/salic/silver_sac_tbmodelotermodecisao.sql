-- Silver SALIC — silver_sac_tbmodelotermodecisao (ex-sac__tbmodelotermodecisao).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_tbmodelotermodecisao, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 6 colunas: 4 tipadas, 1 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idmodelotermodecisao") }} as idmodelotermodecisao,
    {{ bronze_inteiro("idorgao") }} as idorgao,
    {{ bronze_inteiro("idverificacao") }} as idverificacao,
    {{ bronze_booleano("stmodelotermodecisao") }} as stmodelotermodecisao,
    {{ bronze_texto("memodelotermodecisao") }} as memodelotermodecisao,
    _fatia
from {{ source("salic", "bronze_sac_tbmodelotermodecisao") }}
