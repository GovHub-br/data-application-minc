-- Bronze SALIC — sac__tbprogramarouanetareasegmento.
-- Origem: salic_bronze.sac__tbprogramarouanetareasegmento, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 5 colunas: 2 tipadas, 2 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idprogramarouanetareasegmento") }}
    as idprogramarouanetareasegmento,
    {{ bronze_inteiro("idprogramarouanet") }} as idprogramarouanet,
    {{ bronze_texto("cdarea") }} as cdarea,
    {{ bronze_texto("cdsegmento") }} as cdsegmento,
    _fatia
from {{ source("bronze_sac", "sac__tbprogramarouanetareasegmento") }}
