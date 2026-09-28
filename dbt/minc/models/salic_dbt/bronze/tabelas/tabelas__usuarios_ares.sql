-- Bronze SALIC — tabelas__usuarios_ares.
-- Origem: salic_bronze.tabelas__usuarios_ares, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 26 colunas: 13 tipadas, 12 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("usu_codigo") }} as usu_codigo,
    {{ bronze_texto("usu_identificacao") }} as usu_identificacao,
    {{ bronze_texto("usu_nome") }} as usu_nome,
    {{ bronze_inteiro("usu_pessoa") }} as usu_pessoa,
    {{ bronze_inteiro("usu_orgao") }} as usu_orgao,
    {{ bronze_texto("usu_sala") }} as usu_sala,
    {{ bronze_inteiro("usu_ramal") }} as usu_ramal,
    {{ bronze_inteiro("usu_nivel") }} as usu_nivel,
    {{ bronze_texto("usu_exibicao") }} as usu_exibicao,
    {{ bronze_texto("usu_sql_login") }} as usu_sql_login,
    {{ bronze_texto("usu_sql_senha") }} as usu_sql_senha,
    {{ bronze_inteiro("usu_duracao_senha") }} as usu_duracao_senha,
    {{ bronze_timestamp("usu_data_validade") }} as usu_data_validade,
    {{ bronze_timestamp("usu_limite_utilizacao") }} as usu_limite_utilizacao,
    {{ bronze_texto("usu_senha") }} as usu_senha,
    {{ bronze_texto("usu_validacao") }} as usu_validacao,
    {{ bronze_inteiro("usu_status") }} as usu_status,
    {{ bronze_texto("usu_seguranca") }} as usu_seguranca,
    {{ bronze_timestamp("usu_data_atualizacao") }} as usu_data_atualizacao,
    {{ bronze_inteiro("usu_conta_nt") }} as usu_conta_nt,
    {{ bronze_inteiro("usu_dica_intranet") }} as usu_dica_intranet,
    {{ bronze_texto("usu_controle") }} as usu_controle,
    {{ bronze_inteiro("usu_localizacao") }} as usu_localizacao,
    {{ bronze_texto("usu_andar") }} as usu_andar,
    {{ bronze_texto("usu_telefone") }} as usu_telefone,
    _fatia
from {{ source("bronze_tabelas", "tabelas__usuarios_ares") }}
