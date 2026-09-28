-- Bronze SALIC — sac__auditoria.
-- Origem: salic_bronze.sac__auditoria, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 6 colunas: 0 tipadas, 5 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("database_name") }} as database_name,
    {{ bronze_texto("schema_name") }} as schema_name,
    {{ bronze_texto("class_type") }} as class_type,
    {{ bronze_texto("object_name") }} as object_name,
    {{ bronze_texto("statement") }} as statement,
    _fatia
from {{ source("bronze_sac", "sac__auditoria") }}
