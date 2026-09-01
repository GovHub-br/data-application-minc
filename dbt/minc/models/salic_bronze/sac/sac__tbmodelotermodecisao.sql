-- Bronze SALIC — sac__tbmodelotermodecisao.
-- Origem: salic_bronze.sac__tbmodelotermodecisao, onde tudo chega como texto da
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
from {{ source("bronze_sac", "sac__tbmodelotermodecisao") }}
