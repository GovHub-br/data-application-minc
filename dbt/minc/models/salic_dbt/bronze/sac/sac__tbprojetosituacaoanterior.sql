-- Bronze SALIC — sac__tbprojetosituacaoanterior.
-- Origem: salic_bronze.sac__tbprojetosituacaoanterior, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 13 colunas: 10 tipadas, 2 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idprojetosituacaoanterior") }} as idprojetosituacaoanterior,
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_texto("cdsituacaoanterior") }} as cdsituacaoanterior,
    {{ bronze_timestamp("dtsituacaoanterior") }} as dtsituacaoanterior,
    {{ bronze_texto("dsprovidenciaanterior") }} as dsprovidenciaanterior,
    {{ bronze_inteiro("idorgaoanterior") }} as idorgaoanterior,
    {{ bronze_inteiro("idusuarioanterior") }} as idusuarioanterior,
    {{ bronze_inteiro("idacao") }} as idacao,
    {{ bronze_inteiro("tpacao") }} as tpacao,
    {{ bronze_timestamp("dtacao") }} as dtacao,
    {{ bronze_inteiro("idusuario") }} as idusuario,
    {{ bronze_booleano("stestado") }} as stestado,
    _fatia
from {{ source("bronze_sac", "sac__tbprojetosituacaoanterior") }}
