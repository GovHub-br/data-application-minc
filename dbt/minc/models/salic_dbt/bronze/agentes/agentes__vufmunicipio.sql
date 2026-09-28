-- Bronze SALIC — agentes__vufmunicipio.
-- Origem: salic_bronze.agentes__vufmunicipio, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 5 colunas: 2 tipadas, 2 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("iduf") }} as iduf,
    {{ bronze_texto("uf") }} as uf,
    {{ bronze_inteiro("idmunicipio") }} as idmunicipio,
    {{ bronze_texto("municipio") }} as municipio,
    _fatia
from {{ source("bronze_agentes", "agentes__vufmunicipio") }}
