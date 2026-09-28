-- Bronze SALIC — tabelas__pessoasxfuncoes.
-- Origem: salic_bronze.tabelas__pessoasxfuncoes, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 5 colunas: 4 tipadas, 0 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("pxf_pessoa") }} as pxf_pessoa,
    {{ bronze_inteiro("pxf_funcao") }} as pxf_funcao,
    {{ bronze_inteiro("pxf_entidade") }} as pxf_entidade,
    {{ bronze_inteiro("pxf_status") }} as pxf_status,
    _fatia
from {{ source("bronze_tabelas", "tabelas__pessoasxfuncoes") }}
