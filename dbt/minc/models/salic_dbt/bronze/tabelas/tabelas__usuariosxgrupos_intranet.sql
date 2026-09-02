-- Bronze SALIC — tabelas__usuariosxgrupos_intranet.
-- Origem: salic_bronze.tabelas__usuariosxgrupos_intranet, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 3 colunas: 2 tipadas, 0 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("uxg_usuario") }} as uxg_usuario,
    {{ bronze_inteiro("uxg_grupo") }} as uxg_grupo,
    _fatia
from {{ source("bronze_tabelas", "tabelas__usuariosxgrupos_intranet") }}
