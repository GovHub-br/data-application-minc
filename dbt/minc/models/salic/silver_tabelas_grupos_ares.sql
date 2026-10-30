-- Silver SALIC — silver_tabelas_grupos_ares (ex-tabelas__grupos_ares).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_tabelas_grupos_ares, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 5 colunas: 3 tipadas, 1 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("gru_codigo") }} as gru_codigo,
    {{ bronze_inteiro("gru_sistema") }} as gru_sistema,
    {{ bronze_texto("gru_nome") }} as gru_nome,
    {{ bronze_inteiro("gru_status") }} as gru_status,
    _fatia
from {{ source("salic", "bronze_tabelas_grupos_ares") }}
