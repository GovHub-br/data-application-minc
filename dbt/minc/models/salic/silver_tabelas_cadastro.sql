-- Silver SALIC — silver_tabelas_cadastro (ex-tabelas__cadastro).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_tabelas_cadastro, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 5 colunas: 0 tipadas, 4 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("nome") }} as nome,
    {{ bronze_texto("lotacao") }} as lotacao,
    {{ bronze_texto("telefone") }} as telefone,
    {{ bronze_texto("email") }} as email,
    _fatia
from {{ source("salic", "bronze_tabelas_cadastro") }}
