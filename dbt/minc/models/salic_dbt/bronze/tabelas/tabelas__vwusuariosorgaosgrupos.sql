-- Bronze SALIC — tabelas__vwusuariosorgaosgrupos.
-- Origem: salic_bronze.tabelas__vwusuariosorgaosgrupos, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 18 colunas: 7 tipadas, 10 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("usu_codigo") }} as usu_codigo,
    {{ bronze_texto("usu_identificacao") }} as usu_identificacao,
    {{ bronze_texto("usu_nome") }} as usu_nome,
    {{ bronze_inteiro("usu_orgao") }} as usu_orgao,
    {{ bronze_texto("usu_orgaolotacao") }} as usu_orgaolotacao,
    {{ bronze_texto("usu_telefone") }} as usu_telefone,
    {{ bronze_inteiro("org_superior") }} as org_superior,
    {{ bronze_inteiro("uog_orgao") }} as uog_orgao,
    {{ bronze_texto("org_siglaautorizado") }} as org_siglaautorizado,
    {{ bronze_texto("org_nomeautorizado") }} as org_nomeautorizado,
    {{ bronze_inteiro("sis_codigo") }} as sis_codigo,
    {{ bronze_texto("sis_sigla") }} as sis_sigla,
    {{ bronze_texto("sis_nome") }} as sis_nome,
    {{ bronze_inteiro("gru_codigo") }} as gru_codigo,
    {{ bronze_texto("gru_nome") }} as gru_nome,
    {{ bronze_texto("uog_status") }} as uog_status,
    {{ bronze_inteiro("id_unico", tipo="bigint") }} as id_unico,
    _fatia
from {{ source("bronze_tabelas", "tabelas__vwusuariosorgaosgrupos") }}
