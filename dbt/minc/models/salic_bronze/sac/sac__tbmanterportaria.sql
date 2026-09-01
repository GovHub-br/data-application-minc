-- Bronze SALIC — sac__tbmanterportaria.
-- Origem: salic_bronze.sac__tbmanterportaria, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 7 colunas: 3 tipadas, 3 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idmanterportaria") }} as idmanterportaria,
    {{ bronze_texto("dsassinante") }} as dsassinante,
    {{ bronze_timestamp("dtportariapublicacao") }} as dtportariapublicacao,
    {{ bronze_booleano("stestado") }} as stestado,
    {{ bronze_texto("dscargo") }} as dscargo,
    {{ bronze_texto("dsportaria") }} as dsportaria,
    _fatia
from {{ source("bronze_sac", "sac__tbmanterportaria") }}
