-- Bronze SALIC — sac__tbcomprovacaopassivo.
-- Origem: salic_bronze.sac__tbcomprovacaopassivo, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 13 colunas: 9 tipadas, 3 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idcomprovacaopassivo") }} as idcomprovacaopassivo,
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_inteiro("idplanilhaaprovacao") }} as idplanilhaaprovacao,
    {{ bronze_inteiro("idlancamentobancario") }} as idlancamentobancario,
    {{ bronze_inteiro("idfornecedor") }} as idfornecedor,
    {{ bronze_texto("tpcomprovante") }} as tpcomprovante,
    {{ bronze_texto("nrcomprovante") }} as nrcomprovante,
    {{ bronze_timestamp("dtemissaocomprovante") }} as dtemissaocomprovante,
    {{ bronze_numerico("vlcomprovante") }} as vlcomprovante,
    {{ bronze_texto("dsjustificativa") }} as dsjustificativa,
    {{ bronze_inteiro("idarquivo") }} as idarquivo,
    {{ bronze_inteiro("idusuario") }} as idusuario,
    _fatia
from {{ source("bronze_sac", "sac__tbcomprovacaopassivo") }}
