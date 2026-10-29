-- Silver SALIC — silver_tabelas_menusxgrupos_minc11 (ex-tabelas__menusxgrupos_minc11).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_tabelas_menusxgrupos_minc11, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 3 colunas: 2 tipadas, 0 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("mxg_menu") }} as mxg_menu,
    {{ bronze_inteiro("mxg_grupo") }} as mxg_grupo,
    _fatia
from {{ source("salic", "bronze_tabelas_menusxgrupos_minc11") }}
