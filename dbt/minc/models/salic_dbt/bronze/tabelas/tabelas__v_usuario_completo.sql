-- Bronze SALIC — tabelas__v_usuario_completo.
-- Origem: salic_bronze.tabelas__v_usuario_completo, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 25 colunas: 5 tipadas, 19 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("usu_codigo") }} as usu_codigo,
    {{ bronze_texto("usu_identificacao") }} as usu_identificacao,
    {{ bronze_texto("usu_nome") }} as usu_nome,
    {{ bronze_inteiro("usu_pessoa") }} as usu_pessoa,
    {{ bronze_inteiro("usu_orgao") }} as usu_orgao,
    {{ bronze_texto("usu_sala") }} as usu_sala,
    {{ bronze_texto("usu_ramal") }} as usu_ramal,
    {{ bronze_texto("usu_nivel") }} as usu_nivel,
    {{ bronze_texto("usu_exibicao") }} as usu_exibicao,
    {{ bronze_texto("usu_sql_login") }} as usu_sql_login,
    {{ bronze_texto("usu_sql_senha") }} as usu_sql_senha,
    {{ bronze_texto("usu_duracao_senha") }} as usu_duracao_senha,
    {{ bronze_timestamp("usu_data_validade") }} as usu_data_validade,
    {{ bronze_timestamp("usu_limite_utilizacao") }} as usu_limite_utilizacao,
    {{ bronze_texto("usu_senha") }} as usu_senha,
    {{ bronze_texto("usu_status") }} as usu_status,
    {{ bronze_texto("usu_validacao") }} as usu_validacao,
    {{ bronze_texto("usu_seguranca") }} as usu_seguranca,
    {{ bronze_texto("usu_flag_validacao") }} as usu_flag_validacao,
    {{ bronze_texto("usu_flag_seguranca") }} as usu_flag_seguranca,
    {{ bronze_texto("usu_nome_completo") }} as usu_nome_completo,
    {{ bronze_texto("org_sigla") }} as org_sigla,
    {{ bronze_texto("org_nome_completo") }} as org_nome_completo,
    {{ bronze_texto("org_estrutura") }} as org_estrutura,
    _fatia
from {{ source("bronze_tabelas", "tabelas__v_usuario_completo") }}
