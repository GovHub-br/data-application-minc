-- Bronze SALIC — sac__tbprogramarouanetavaliador.
-- Origem: salic_bronze.sac__tbprogramarouanetavaliador, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 6 colunas: 3 tipadas, 2 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idprogramarouanetavaliador") }} as idprogramarouanetavaliador,
    {{ bronze_inteiro("idprogramarouanet") }} as idprogramarouanet,
    {{ bronze_inteiro("idavaliador") }} as idavaliador,
    {{ bronze_texto("cdarea") }} as cdarea,
    {{ bronze_texto("cdsegmento") }} as cdsegmento,
    _fatia
from {{ source("bronze_sac", "sac__tbprogramarouanetavaliador") }}
