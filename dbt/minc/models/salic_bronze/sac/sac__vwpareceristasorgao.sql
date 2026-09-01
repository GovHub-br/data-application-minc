-- Bronze SALIC — sac__vwpareceristasorgao.
-- Origem: salic_bronze.sac__vwpareceristasorgao, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 4 colunas: 2 tipadas, 1 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idparecerista") }} as idparecerista,
    {{ bronze_texto("nome") }} as nome,
    {{ bronze_inteiro("idorgao") }} as idorgao,
    _fatia
from {{ source("bronze_sac", "sac__vwpareceristasorgao") }}
