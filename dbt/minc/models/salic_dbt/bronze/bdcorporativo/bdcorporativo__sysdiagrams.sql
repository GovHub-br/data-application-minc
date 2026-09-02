-- Bronze SALIC — bdcorporativo__sysdiagrams.
-- Origem: salic_bronze.bdcorporativo__sysdiagrams, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 6 colunas: 3 tipadas, 2 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("name") }} as name,
    {{ bronze_inteiro("principal_id") }} as principal_id,
    {{ bronze_inteiro("diagram_id") }} as diagram_id,
    {{ bronze_inteiro("version") }} as version,
    {{ bronze_texto("definition") }} as definition,
    _fatia
from {{ source("bronze_bdcorporativo", "bdcorporativo__sysdiagrams") }}
