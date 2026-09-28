-- Bronze SALIC — tabelas__v_usuario_orgaos_grupos.
-- Origem: salic_bronze.tabelas__v_usuario_orgaos_grupos, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 20 colunas: 11 tipadas, 8 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("usu_codigo") }} as usu_codigo,
    {{ bronze_texto("usu_identificacao") }} as usu_identificacao,
    {{ bronze_texto("usu_nome") }} as usu_nome,
    {{ bronze_texto("usu_nome_completo") }} as usu_nome_completo,
    {{ bronze_inteiro("org_codigo") }} as org_codigo,
    {{ bronze_inteiro("org_pessoa") }} as org_pessoa,
    {{ bronze_inteiro("org_gerente") }} as org_gerente,
    {{ bronze_inteiro("org_superior") }} as org_superior,
    {{ bronze_texto("org_sigla") }} as org_sigla,
    {{ bronze_inteiro("org_cei") }} as org_cei,
    {{ bronze_inteiro("org_tipo") }} as org_tipo,
    {{ bronze_texto("org_status") }} as org_status,
    {{ bronze_texto("org_nome_completo") }} as org_nome_completo,
    {{ bronze_inteiro("uog_usuario") }} as uog_usuario,
    {{ bronze_inteiro("uog_orgao") }} as uog_orgao,
    {{ bronze_inteiro("uog_grupo") }} as uog_grupo,
    {{ bronze_inteiro("gru_sistema") }} as gru_sistema,
    {{ bronze_texto("gru_nome") }} as gru_nome,
    {{ bronze_texto("gru_status") }} as gru_status,
    _fatia
from {{ source("bronze_tabelas", "tabelas__v_usuario_orgaos_grupos") }}
