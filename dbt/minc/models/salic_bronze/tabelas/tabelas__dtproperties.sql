-- Bronze SALIC — tabelas__dtproperties.
-- Origem: salic_bronze.tabelas__dtproperties, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 8 colunas: 3 tipadas, 4 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("id") }} as id,
    {{ bronze_inteiro("objectid") }} as objectid,
    {{ bronze_texto("property") }} as property,
    {{ bronze_texto("value") }} as value,
    {{ bronze_texto("uvalue") }} as uvalue,
    {{ bronze_texto("lvalue") }} as lvalue,
    {{ bronze_inteiro("version") }} as version,
    _fatia
from {{ source("bronze_tabelas", "tabelas__dtproperties") }}
