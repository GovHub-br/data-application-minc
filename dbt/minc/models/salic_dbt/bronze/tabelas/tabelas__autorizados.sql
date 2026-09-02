-- Bronze SALIC — tabelas__autorizados.
-- Origem: salic_bronze.tabelas__autorizados, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 6 colunas: 2 tipadas, 3 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("aut_identificacao") }} as aut_identificacao,
    {{ bronze_texto("aut_procedure") }} as aut_procedure,
    {{ bronze_texto("aut_operacao") }} as aut_operacao,
    {{ bronze_inteiro("aut_codigo") }} as aut_codigo,
    {{ bronze_inteiro("aut_status") }} as aut_status,
    _fatia
from {{ source("bronze_tabelas", "tabelas__autorizados") }}
