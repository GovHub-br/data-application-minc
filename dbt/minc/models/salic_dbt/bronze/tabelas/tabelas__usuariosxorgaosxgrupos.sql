-- Bronze SALIC — tabelas__usuariosxorgaosxgrupos.
-- Origem: salic_bronze.tabelas__usuariosxorgaosxgrupos, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 5 colunas: 3 tipadas, 1 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("uog_usuario") }} as uog_usuario,
    {{ bronze_inteiro("uog_orgao") }} as uog_orgao,
    {{ bronze_inteiro("uog_grupo") }} as uog_grupo,
    {{ bronze_texto("uog_status") }} as uog_status,
    _fatia
from {{ source("bronze_tabelas", "tabelas__usuariosxorgaosxgrupos") }}
