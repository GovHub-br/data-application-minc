-- Silver SALIC — silver_tabelas_palavras (ex-tabelas__palavras).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_tabelas_palavras, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 4 colunas: 2 tipadas, 1 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("pal_codigo") }} as pal_codigo,
    {{ bronze_texto("pal_texto") }} as pal_texto,
    {{ bronze_inteiro("pal_status") }} as pal_status,
    _fatia
from {{ source("salic", "bronze_tabelas_palavras") }}
