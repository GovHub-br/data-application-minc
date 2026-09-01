-- Bronze SALIC — sac__tbhomologacao.
-- Origem: salic_bronze.sac__tbhomologacao, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 8 colunas: 4 tipadas, 3 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idhomologacao") }} as idhomologacao,
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_texto("tphomologacao") }} as tphomologacao,
    {{ bronze_timestamp("dthomologacao") }} as dthomologacao,
    {{ bronze_texto("stdecisao") }} as stdecisao,
    {{ bronze_texto("dshomologacao") }} as dshomologacao,
    {{ bronze_inteiro("idusuario") }} as idusuario,
    _fatia
from {{ source("bronze_sac", "sac__tbhomologacao") }}
