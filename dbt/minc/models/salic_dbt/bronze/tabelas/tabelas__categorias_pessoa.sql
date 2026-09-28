-- Bronze SALIC — tabelas__categorias_pessoa.
-- Origem: salic_bronze.tabelas__categorias_pessoa, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 5 colunas: 3 tipadas, 1 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("ctp_codigo") }} as ctp_codigo,
    {{ bronze_texto("ctp_descricao") }} as ctp_descricao,
    {{ bronze_inteiro("ctp_orgao_gerente") }} as ctp_orgao_gerente,
    {{ bronze_inteiro("ctp_status") }} as ctp_status,
    _fatia
from {{ source("bronze_tabelas", "tabelas__categorias_pessoa") }}
