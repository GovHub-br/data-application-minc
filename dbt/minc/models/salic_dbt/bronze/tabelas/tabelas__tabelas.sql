-- Bronze SALIC — tabelas__tabelas.
-- Origem: salic_bronze.tabelas__tabelas, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 5 colunas: 3 tipadas, 1 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("tab_tipo") }} as tab_tipo,
    {{ bronze_inteiro("tab_codigo") }} as tab_codigo,
    {{ bronze_texto("tab_descricao") }} as tab_descricao,
    {{ bronze_inteiro("tab_status") }} as tab_status,
    _fatia
from {{ source("bronze_tabelas", "tabelas__tabelas") }}
