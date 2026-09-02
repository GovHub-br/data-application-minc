-- Bronze SALIC — tabelas__vwusuarios.
-- Origem: salic_bronze.tabelas__vwusuarios, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 12 colunas: 3 tipadas, 8 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("usu_codigo") }} as usu_codigo,
    {{ bronze_texto("usu_identificacao") }} as usu_identificacao,
    {{ bronze_texto("usu_nome") }} as usu_nome,
    {{ bronze_inteiro("usu_orgao") }} as usu_orgao,
    {{ bronze_texto("usu_lotacao") }} as usu_lotacao,
    {{ bronze_texto("usu_status") }} as usu_status,
    {{ bronze_inteiro("gru_orgao") }} as gru_orgao,
    {{ bronze_texto("gru_sigla") }} as gru_sigla,
    {{ bronze_texto("usu_localizacao") }} as usu_localizacao,
    {{ bronze_texto("usu_andar") }} as usu_andar,
    {{ bronze_texto("usu_telefone") }} as usu_telefone,
    _fatia
from {{ source("bronze_tabelas", "tabelas__vwusuarios") }}
