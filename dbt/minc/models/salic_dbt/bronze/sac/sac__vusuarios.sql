-- Bronze SALIC — sac__vusuarios.
-- Origem: salic_bronze.sac__vusuarios, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 18 colunas: 5 tipadas, 12 mantidas como texto.
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
    {{ bronze_inteiro("org_codigo") }} as org_codigo,
    {{ bronze_texto("org_sigla") }} as org_sigla,
    {{ bronze_texto("org_estrutura") }} as org_estrutura,
    {{ bronze_texto("org_nome") }} as org_nome,
    {{ bronze_inteiro("org_gerente") }} as org_gerente,
    {{ bronze_texto("org_nomegerente") }} as org_nomegerente,
    {{ bronze_texto("fun_descricao") }} as fun_descricao,
    {{ bronze_texto("fun_funcaoorgao") }} as fun_funcaoorgao,
    _fatia
from {{ source("bronze_sac", "sac__vusuarios") }}
