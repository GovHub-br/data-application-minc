-- Bronze SALIC — sac__tbdocumentoassinatura.
-- Origem: salic_bronze.sac__tbdocumentoassinatura, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 10 colunas: 8 tipadas, 1 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("iddocumentoassinatura") }} as iddocumentoassinatura,
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_inteiro("idtipodoatoadministrativo") }} as idtipodoatoadministrativo,
    {{ bronze_texto("conteudo") }} as conteudo,
    {{ bronze_timestamp("dt_criacao") }} as dt_criacao,
    {{ bronze_inteiro("idcriadordocumento") }} as idcriadordocumento,
    {{ bronze_inteiro("cdsituacao") }} as cdsituacao,
    {{ bronze_inteiro("idatodegestao") }} as idatodegestao,
    {{ bronze_booleano("stestado") }} as stestado,
    _fatia
from {{ source("bronze_sac", "sac__tbdocumentoassinatura") }}
