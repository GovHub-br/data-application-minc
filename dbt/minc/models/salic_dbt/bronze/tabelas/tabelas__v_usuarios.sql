-- Bronze SALIC — tabelas__v_usuarios.
-- Origem: salic_bronze.tabelas__v_usuarios, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 10 colunas: 3 tipadas, 6 mantidas como texto.
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
    {{ bronze_texto("usu_nome_completo") }} as usu_nome_completo,
    _fatia
from {{ source("bronze_tabelas", "tabelas__v_usuarios") }}
