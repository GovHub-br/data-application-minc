-- Bronze SALIC — tabelas__tipos_logradouro.
-- Origem: salic_bronze.tabelas__tipos_logradouro, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 4 colunas: 1 tipadas, 2 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("tlg_codigo") }} as tlg_codigo,
    {{ bronze_texto("tlg_sigla") }} as tlg_sigla,
    {{ bronze_texto("tlg_descricao") }} as tlg_descricao,
    _fatia
from {{ source("bronze_tabelas", "tabelas__tipos_logradouro") }}
