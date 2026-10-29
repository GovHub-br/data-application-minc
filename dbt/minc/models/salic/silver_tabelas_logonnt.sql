-- Silver SALIC — silver_tabelas_logonnt (ex-tabelas__logonnt).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_tabelas_logonnt, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 4 colunas: 2 tipadas, 1 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("name") }} as name,
    {{ bronze_inteiro("status", tipo="bigint") }} as status,
    {{ bronze_timestamp("data") }} as data,
    _fatia
from {{ source("salic", "bronze_tabelas_logonnt") }}
