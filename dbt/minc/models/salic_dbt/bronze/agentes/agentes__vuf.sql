-- Bronze SALIC — agentes__vuf.
-- Origem: salic_bronze.agentes__vuf, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 5 colunas: 1 tipadas, 3 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("iduf") }} as iduf,
    {{ bronze_texto("sigla") }} as sigla,
    {{ bronze_texto("descricao") }} as descricao,
    {{ bronze_texto("regiao") }} as regiao,
    _fatia
from {{ source("bronze_agentes", "agentes__vuf") }}
