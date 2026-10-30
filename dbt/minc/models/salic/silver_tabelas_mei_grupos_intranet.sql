-- Silver SALIC — silver_tabelas_mei_grupos_intranet (ex-tabelas__mei_grupos_intranet).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_tabelas_mei_grupos_intranet, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 3 colunas: 1 tipadas, 1 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("gri_codigo") }} as gri_codigo,
    {{ bronze_texto("gri_grupo") }} as gri_grupo,
    _fatia
from {{ source("salic", "bronze_tabelas_mei_grupos_intranet") }}
