-- Bronze SALIC — sac__vwpaineldetransferenciaderecursos.
-- Origem: salic_bronze.sac__vwpaineldetransferenciaderecursos, onde tudo chega como
-- texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 18 colunas: 3 tipadas, 14 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("pronac") }} as pronac,
    {{ bronze_texto("cgccpfmecena") }} as cgccpfmecena,
    {{ bronze_texto("dtchegadarecibo") }} as dtchegadarecibo,
    {{ bronze_texto("dtrecibo") }} as dtrecibo,
    {{ bronze_numerico("captacaoreal") }} as captacaoreal,
    {{ bronze_texto("numerorecibo") }} as numerorecibo,
    {{ bronze_texto("tipoapoio") }} as tipoapoio,
    {{ bronze_texto("incentivador") }} as incentivador,
    {{ bronze_texto("dtliberacao") }} as dtliberacao,
    {{ bronze_texto("inabilitado") }} as inabilitado,
    {{ bronze_texto("certidao") }} as certidao,
    {{ bronze_texto("cadin") }} as cadin,
    {{ bronze_numerico("percentual") }} as percentual,
    {{ bronze_texto("idcaptacao") }} as idcaptacao,
    {{ bronze_texto("idpronac") }} as idpronac,
    {{ bronze_texto("situacao") }} as situacao,
    {{ bronze_texto("idsecretaria") }} as idsecretaria,
    _fatia
from {{ source("bronze_sac", "sac__vwpaineldetransferenciaderecursos") }}
