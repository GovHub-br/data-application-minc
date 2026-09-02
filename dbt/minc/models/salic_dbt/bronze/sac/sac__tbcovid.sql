-- Bronze SALIC — sac__tbcovid.
-- Origem: salic_bronze.sac__tbcovid, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 8 colunas: 4 tipadas, 3 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idcovid") }} as idcovid,
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_texto("tpprocedimento") }} as tpprocedimento,
    {{ bronze_timestamp("dtprocedimento") }} as dtprocedimento,
    {{ bronze_texto("dsmotivacao") }} as dsmotivacao,
    {{ bronze_texto("dsalcancedaacao") }} as dsalcancedaacao,
    {{ bronze_inteiro("idusuario") }} as idusuario,
    _fatia
from {{ source("bronze_sac", "sac__tbcovid") }}
