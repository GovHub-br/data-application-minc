-- Bronze SALIC — tabelas__palavras.
-- Origem: salic_bronze.tabelas__palavras, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 4 colunas: 2 tipadas, 1 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("pal_codigo") }} as pal_codigo,
    {{ bronze_texto("pal_texto") }} as pal_texto,
    {{ bronze_inteiro("pal_status") }} as pal_status,
    _fatia
from {{ source("bronze_tabelas", "tabelas__palavras") }}
