-- Bronze SALIC — sac__tbpareceristaedital.
-- Origem: salic_bronze.sac__tbpareceristaedital, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 10 colunas: 8 tipadas, 1 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("nredital") }} as nredital,
    {{ bronze_timestamp("dtiniciovigencia") }} as dtiniciovigencia,
    {{ bronze_timestamp("dtfimvigencia") }} as dtfimvigencia,
    {{ bronze_timestamp("dtcredenciamento") }} as dtcredenciamento,
    {{ bronze_timestamp("dtdescredenciamento") }} as dtdescredenciamento,
    {{ bronze_booleano("stestado") }} as stestado,
    {{ bronze_inteiro("idusuario") }} as idusuario,
    {{ bronze_inteiro("idagente") }} as idagente,
    {{ bronze_inteiro("idpareceristasedital") }} as idpareceristasedital,
    _fatia
from {{ source("bronze_sac", "sac__tbpareceristaedital") }}
