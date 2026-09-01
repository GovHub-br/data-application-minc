-- Bronze SALIC — tabelas__localidades.
-- Origem: salic_bronze.tabelas__localidades, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 8 colunas: 3 tipadas, 4 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("loc_codigo") }} as loc_codigo,
    {{ bronze_texto("loc_estruturado") }} as loc_estruturado,
    {{ bronze_inteiro("loc_tipo") }} as loc_tipo,
    {{ bronze_texto("loc_nome") }} as loc_nome,
    {{ bronze_texto("loc_sigla") }} as loc_sigla,
    {{ bronze_texto("loc_cep") }} as loc_cep,
    {{ bronze_inteiro("loc_status") }} as loc_status,
    _fatia
from {{ source("bronze_tabelas", "tabelas__localidades") }}
