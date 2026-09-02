-- Bronze SALIC — sac__vwprogramarouanetavaliadores.
-- Origem: salic_bronze.sac__vwprogramarouanetavaliadores, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 12 colunas: 3 tipadas, 8 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idprogramarouanet") }} as idprogramarouanet,
    {{ bronze_inteiro("idagente") }} as idagente,
    {{ bronze_texto("cpf") }} as cpf,
    {{ bronze_texto("nome") }} as nome,
    {{ bronze_inteiro("idprogramarouanetavaliador") }} as idprogramarouanetavaliador,
    {{ bronze_texto("qtavaliadores") }} as qtavaliadores,
    {{ bronze_texto("qtpropostas") }} as qtpropostas,
    {{ bronze_texto("cdarea") }} as cdarea,
    {{ bronze_texto("dsarea") }} as dsarea,
    {{ bronze_texto("cdsegmento") }} as cdsegmento,
    {{ bronze_texto("dssegmento") }} as dssegmento,
    _fatia
from {{ source("bronze_sac", "sac__vwprogramarouanetavaliadores") }}
