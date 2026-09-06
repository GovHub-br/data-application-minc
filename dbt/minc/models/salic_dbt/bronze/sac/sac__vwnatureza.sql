-- Bronze SALIC — sac__vwnatureza.
-- Origem: salic_bronze.sac__vwnatureza, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 7 colunas: 2 tipadas, 4 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idagente") }} as idagente,
    {{ bronze_texto("direito") }} as direito,
    {{ bronze_texto("esfera") }} as esfera,
    {{ bronze_texto("poder") }} as poder,
    {{ bronze_texto("administracao") }} as administracao,
    {{ bronze_inteiro("usuario") }} as usuario,
    _fatia
from {{ source("bronze_sac", "sac__vwnatureza") }}
