-- Bronze SALIC — sac__vwinformacoesdoatoadministrativo.
-- Origem: salic_bronze.sac__vwinformacoesdoatoadministrativo, onde tudo chega como
-- texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 8 colunas: 7 tipadas, 0 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_inteiro("idtipodoatoadministrativo") }} as idtipodoatoadministrativo,
    {{ bronze_inteiro("idatoadministrativo") }} as idatoadministrativo,
    {{ bronze_inteiro("iddocumentoassinatura") }} as iddocumentoassinatura,
    {{ bronze_timestamp("dt_criacao") }} as dt_criacao,
    {{ bronze_timestamp("dtassinatura") }} as dtassinatura,
    {{ bronze_inteiro("idatodegestao") }} as idatodegestao,
    _fatia
from {{ source("bronze_sac", "sac__vwinformacoesdoatoadministrativo") }}
