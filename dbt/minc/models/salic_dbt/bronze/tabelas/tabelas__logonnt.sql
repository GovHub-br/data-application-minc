-- Bronze SALIC — tabelas__logonnt.
-- Origem: salic_bronze.tabelas__logonnt, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 4 colunas: 2 tipadas, 1 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("name") }} as name,
    {{ bronze_inteiro("status", tipo="bigint") }} as status,
    {{ bronze_timestamp("data") }} as data,
    _fatia
from {{ source("bronze_tabelas", "tabelas__logonnt") }}
