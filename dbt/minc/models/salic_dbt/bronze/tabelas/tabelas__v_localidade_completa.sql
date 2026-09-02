-- Bronze SALIC — tabelas__v_localidade_completa.
-- Origem: salic_bronze.tabelas__v_localidade_completa, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 10 colunas: 2 tipadas, 7 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("loc_codigo") }} as loc_codigo,
    {{ bronze_texto("loc_estruturado") }} as loc_estruturado,
    {{ bronze_texto("loc_tipo") }} as loc_tipo,
    {{ bronze_texto("loc_sigla") }} as loc_sigla,
    {{ bronze_inteiro("loc_cep") }} as loc_cep,
    {{ bronze_texto("loc_pais") }} as loc_pais,
    {{ bronze_texto("loc_estado") }} as loc_estado,
    {{ bronze_texto("loc_municipio") }} as loc_municipio,
    {{ bronze_texto("loc_distrito") }} as loc_distrito,
    _fatia
from {{ source("bronze_tabelas", "tabelas__v_localidade_completa") }}
