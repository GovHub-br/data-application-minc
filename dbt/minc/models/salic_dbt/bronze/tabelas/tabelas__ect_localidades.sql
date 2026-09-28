-- Bronze SALIC — tabelas__ect_localidades.
-- Origem: salic_bronze.tabelas__ect_localidades, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 6 colunas: 0 tipadas, 5 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("ectl_uf") }} as ectl_uf,
    {{ bronze_texto("ectl_hash") }} as ectl_hash,
    {{ bronze_texto("ectl_local") }} as ectl_local,
    {{ bronze_texto("ectl_tipo_local") }} as ectl_tipo_local,
    {{ bronze_texto("ectl_estruturado") }} as ectl_estruturado,
    _fatia
from {{ source("bronze_tabelas", "tabelas__ect_localidades") }}
