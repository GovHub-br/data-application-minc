-- Silver SALIC — silver_tabelas_v_usuario_completo (ex-tabelas__v_usuario_completo).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_tabelas_v_usuario_completo, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 21 colunas: 10 tipadas, 10 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
-- Credencial e códigos de segurança derivados da senha (usu_senha,
-- usu_sql_senha, usu_seguranca, usu_validacao) existem na Bronze e não entram
-- nesta silver.
select
    {{ bronze_inteiro("usu_codigo") }} as usu_codigo,
    {{ bronze_texto("usu_identificacao") }} as usu_identificacao,
    {{ bronze_texto("usu_nome") }} as usu_nome,
    {{ bronze_inteiro("usu_pessoa") }} as usu_pessoa,
    {{ bronze_inteiro("usu_orgao") }} as usu_orgao,
    {{ bronze_inteiro("usu_sala") }} as usu_sala,
    {{ bronze_inteiro("usu_ramal") }} as usu_ramal,
    {{ bronze_inteiro("usu_nivel") }} as usu_nivel,
    {{ bronze_texto("usu_exibicao") }} as usu_exibicao,
    {{ bronze_texto("usu_sql_login") }} as usu_sql_login,
    {{ bronze_inteiro("usu_duracao_senha") }} as usu_duracao_senha,
    {{ bronze_timestamp("usu_data_validade") }} as usu_data_validade,
    {{ bronze_timestamp("usu_limite_utilizacao") }} as usu_limite_utilizacao,
    {{ bronze_inteiro("usu_status") }} as usu_status,
    {{ bronze_texto("usu_flag_validacao") }} as usu_flag_validacao,
    {{ bronze_texto("usu_flag_seguranca") }} as usu_flag_seguranca,
    {{ bronze_texto("usu_nome_completo") }} as usu_nome_completo,
    {{ bronze_texto("org_sigla") }} as org_sigla,
    {{ bronze_texto("org_nome_completo") }} as org_nome_completo,
    {{ bronze_texto("org_estrutura") }} as org_estrutura,
    _fatia
from {{ source("salic", "bronze_tabelas_v_usuario_completo") }}
