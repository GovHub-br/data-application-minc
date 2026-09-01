-- Bronze SALIC — tabelas__v_hierarquia_orgaos.
-- Origem: salic_bronze.tabelas__v_hierarquia_orgaos, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 10 colunas: 3 tipadas, 6 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("org_codigo") }} as org_codigo,
    {{ bronze_inteiro("org_superior") }} as org_superior,
    {{ bronze_inteiro("org_nivel") }} as org_nivel,
    {{ bronze_texto("org_sigla") }} as org_sigla,
    {{ bronze_texto("org_status") }} as org_status,
    {{ bronze_texto("org_nome") }} as org_nome,
    {{ bronze_texto("sup_sigla") }} as sup_sigla,
    {{ bronze_texto("sup_status") }} as sup_status,
    {{ bronze_texto("sup_nome") }} as sup_nome,
    _fatia
from {{ source("bronze_tabelas", "tabelas__v_hierarquia_orgaos") }}
