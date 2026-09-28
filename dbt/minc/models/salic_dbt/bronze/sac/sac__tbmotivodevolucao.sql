-- Bronze SALIC — sac__tbmotivodevolucao.
-- Origem: salic_bronze.sac__tbmotivodevolucao, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 6 colunas: 4 tipadas, 1 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idmotivodevolucao") }} as idmotivodevolucao,
    {{ bronze_inteiro("iddocumentoassinatura") }} as iddocumentoassinatura,
    {{ bronze_timestamp("dtdevolucao") }} as dtdevolucao,
    {{ bronze_texto("dsmotivodevolucao") }} as dsmotivodevolucao,
    {{ bronze_inteiro("idusuario") }} as idusuario,
    _fatia
from {{ source("bronze_sac", "sac__tbmotivodevolucao") }}
