-- Bronze SALIC — agentes__vsistema.
-- Origem: salic_bronze.agentes__vsistema, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 4 colunas: 1 tipadas, 2 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idsistema") }} as idsistema,
    {{ bronze_texto("descricao") }} as descricao,
    {{ bronze_texto("usuario") }} as usuario,
    _fatia
from {{ source("bronze_agentes", "agentes__vsistema") }}
