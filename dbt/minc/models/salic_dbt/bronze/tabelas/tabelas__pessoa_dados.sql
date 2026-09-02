-- Bronze SALIC — tabelas__pessoa_dados.
-- Origem: salic_bronze.tabelas__pessoa_dados, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 5 colunas: 3 tipadas, 1 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("pdd_pessoa") }} as pdd_pessoa,
    {{ bronze_inteiro("pdd_meta_dado") }} as pdd_meta_dado,
    {{ bronze_inteiro("pdd_sequencia") }} as pdd_sequencia,
    {{ bronze_texto("pdd_dado") }} as pdd_dado,
    _fatia
from {{ source("bronze_tabelas", "tabelas__pessoa_dados") }}
