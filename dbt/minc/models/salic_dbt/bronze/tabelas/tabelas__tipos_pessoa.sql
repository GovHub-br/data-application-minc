-- Bronze SALIC — tabelas__tipos_pessoa.
-- Origem: salic_bronze.tabelas__tipos_pessoa, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 7 colunas: 5 tipadas, 1 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("tpe_codigo") }} as tpe_codigo,
    {{ bronze_texto("tpe_descricao") }} as tpe_descricao,
    {{ bronze_inteiro("tpe_pf_pj") }} as tpe_pf_pj,
    {{ bronze_inteiro("tpe_direito") }} as tpe_direito,
    {{ bronze_inteiro("tpe_fim") }} as tpe_fim,
    {{ bronze_inteiro("tpe_status") }} as tpe_status,
    _fatia
from {{ source("bronze_tabelas", "tabelas__tipos_pessoa") }}
