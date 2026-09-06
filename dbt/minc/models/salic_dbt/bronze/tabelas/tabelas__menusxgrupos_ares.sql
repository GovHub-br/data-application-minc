-- Bronze SALIC — tabelas__menusxgrupos_ares.
-- Origem: salic_bronze.tabelas__menusxgrupos_ares, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 3 colunas: 2 tipadas, 0 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("mxg_menu") }} as mxg_menu,
    {{ bronze_inteiro("mxg_grupo") }} as mxg_grupo,
    _fatia
from {{ source("bronze_tabelas", "tabelas__menusxgrupos_ares") }}
