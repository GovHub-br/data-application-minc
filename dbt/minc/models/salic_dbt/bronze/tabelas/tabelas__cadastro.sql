-- Bronze SALIC — tabelas__cadastro.
-- Origem: salic_bronze.tabelas__cadastro, onde tudo chega como texto da
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
from {{ source("bronze_tabelas", "tabelas__cadastro") }}
