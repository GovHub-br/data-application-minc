-- Silver SALIC — silver_tabelas_usuarios (ex-tabelas__usuarios).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_tabelas_usuarios, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 21 colunas: 14 tipadas, 6 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
-- Credencial e códigos de segurança derivados da senha (usu_senha,
-- usu_sql_senha, usu_seguranca, usu_validacao, usu_controle) existem na Bronze
-- e não entram nesta silver.
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
    {{ bronze_timestamp("usu_data_atualizacao") }} as usu_data_atualizacao,
    {{ bronze_inteiro("usu_conta_nt") }} as usu_conta_nt,
    {{ bronze_inteiro("usu_dica_intranet") }} as usu_dica_intranet,
    {{ bronze_inteiro("usu_localizacao") }} as usu_localizacao,
    {{ bronze_texto("usu_andar") }} as usu_andar,
    {{ bronze_texto("usu_telefone") }} as usu_telefone,
    _fatia
from {{ source("salic", "bronze_tabelas_usuarios") }}
