-- Bronze SALIC — tabelas__orgaos_ares.
-- Origem: salic_bronze.tabelas__orgaos_ares, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 11 colunas: 7 tipadas, 3 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("org_codigo") }} as org_codigo,
    {{ bronze_inteiro("org_pessoa") }} as org_pessoa,
    {{ bronze_inteiro("org_gerente") }} as org_gerente,
    {{ bronze_inteiro("org_superior") }} as org_superior,
    {{ bronze_texto("org_sigla") }} as org_sigla,
    {{ bronze_inteiro("org_cei") }} as org_cei,
    {{ bronze_texto("org_uf") }} as org_uf,
    {{ bronze_inteiro("org_tipo") }} as org_tipo,
    {{ bronze_inteiro("org_status") }} as org_status,
    {{ bronze_texto("org_controle") }} as org_controle,
    _fatia
from {{ source("bronze_tabelas", "tabelas__orgaos_ares") }}
