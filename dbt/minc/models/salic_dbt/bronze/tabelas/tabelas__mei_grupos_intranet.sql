-- Bronze SALIC — tabelas__mei_grupos_intranet.
-- Origem: salic_bronze.tabelas__mei_grupos_intranet, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 3 colunas: 1 tipadas, 1 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("gri_codigo") }} as gri_codigo,
    {{ bronze_texto("gri_grupo") }} as gri_grupo,
    _fatia
from {{ source("bronze_tabelas", "tabelas__mei_grupos_intranet") }}
