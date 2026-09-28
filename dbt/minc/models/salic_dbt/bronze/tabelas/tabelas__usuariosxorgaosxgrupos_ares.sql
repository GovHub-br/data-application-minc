-- Bronze SALIC — tabelas__usuariosxorgaosxgrupos_ares.
-- Origem: salic_bronze.tabelas__usuariosxorgaosxgrupos_ares, onde tudo chega como texto
-- da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 4 colunas: 3 tipadas, 0 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("uog_usuario") }} as uog_usuario,
    {{ bronze_inteiro("uog_orgao") }} as uog_orgao,
    {{ bronze_inteiro("uog_grupo") }} as uog_grupo,
    _fatia
from {{ source("bronze_tabelas", "tabelas__usuariosxorgaosxgrupos_ares") }}
