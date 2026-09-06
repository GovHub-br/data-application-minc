-- Bronze SALIC — sac__tblaudofinal.
-- Origem: salic_bronze.sac__tblaudofinal, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 7 colunas: 4 tipadas, 2 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idlaudofinal") }} as idlaudofinal,
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_timestamp("dtlaudofinal") }} as dtlaudofinal,
    {{ bronze_texto("simanifestacao") }} as simanifestacao,
    {{ bronze_texto("dslaudofinal") }} as dslaudofinal,
    {{ bronze_inteiro("idusuario") }} as idusuario,
    _fatia
from {{ source("bronze_sac", "sac__tblaudofinal") }}
