-- Bronze SALIC — tabelas__headers.
-- Origem: salic_bronze.tabelas__headers, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 7 colunas: 3 tipadas, 3 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("hdr_identificacao") }} as hdr_identificacao,
    {{ bronze_inteiro("hdr_numero") }} as hdr_numero,
    {{ bronze_texto("hdr_texto") }} as hdr_texto,
    {{ bronze_timestamp("hdr_data") }} as hdr_data,
    {{ bronze_inteiro("hdr_flag") }} as hdr_flag,
    {{ bronze_texto("hdr_controle") }} as hdr_controle,
    _fatia
from {{ source("bronze_tabelas", "tabelas__headers") }}
