-- Bronze SALIC — tabelas__emails.
-- Origem: salic_bronze.tabelas__emails, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 11 colunas: 3 tipadas, 7 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("eml_id") }} as eml_id,
    {{ bronze_texto("eml_conta") }} as eml_conta,
    {{ bronze_texto("eml_email") }} as eml_email,
    {{ bronze_texto("eml_dominio") }} as eml_dominio,
    {{ bronze_texto("eml_tipo") }} as eml_tipo,
    {{ bronze_texto("eml_cpf") }} as eml_cpf,
    {{ bronze_inteiro("eml_orgao") }} as eml_orgao,
    {{ bronze_texto("eml_aliases") }} as eml_aliases,
    {{ bronze_texto("eml_utilizacao") }} as eml_utilizacao,
    {{ bronze_inteiro("eml_status") }} as eml_status,
    _fatia
from {{ source("bronze_tabelas", "tabelas__emails") }}
