-- Bronze SALIC — agentes__vvisao.
-- Origem: salic_bronze.agentes__vvisao, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 5 colunas: 2 tipadas, 2 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idvisao") }} as idvisao,
    {{ bronze_inteiro("idagente") }} as idagente,
    {{ bronze_texto("visao") }} as visao,
    {{ bronze_texto("usuario") }} as usuario,
    _fatia
from {{ source("bronze_agentes", "agentes__vvisao") }}
