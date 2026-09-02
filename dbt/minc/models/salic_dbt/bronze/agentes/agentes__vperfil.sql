-- Bronze SALIC — agentes__vperfil.
-- Origem: salic_bronze.agentes__vperfil, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 5 colunas: 1 tipadas, 3 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idagente") }} as idagente,
    {{ bronze_texto("perfil") }} as perfil,
    {{ bronze_texto("caracteristica") }} as caracteristica,
    {{ bronze_texto("usuario") }} as usuario,
    _fatia
from {{ source("bronze_agentes", "agentes__vperfil") }}
