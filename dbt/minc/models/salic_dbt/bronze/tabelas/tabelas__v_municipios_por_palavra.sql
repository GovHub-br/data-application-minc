-- Bronze SALIC — tabelas__v_municipios_por_palavra.
-- Origem: salic_bronze.tabelas__v_municipios_por_palavra, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 6 colunas: 1 tipadas, 4 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("loc_nome") }} as loc_nome,
    {{ bronze_texto("loc_estruturado") }} as loc_estruturado,
    {{ bronze_texto("loc_tipo") }} as loc_tipo,
    {{ bronze_inteiro("loc_codigo") }} as loc_codigo,
    {{ bronze_texto("pal_texto") }} as pal_texto,
    _fatia
from {{ source("bronze_tabelas", "tabelas__v_municipios_por_palavra") }}
