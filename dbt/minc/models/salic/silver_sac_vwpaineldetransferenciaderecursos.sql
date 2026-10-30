-- Silver SALIC — silver_sac_vwpaineldetransferenciaderecursos
-- (ex-sac__vwpaineldetransferenciaderecursos).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_vwpaineldetransferenciaderecursos, onde tudo chega como
-- texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 18 colunas: 5 tipadas, 12 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("pronac") }} as pronac,
    {{ bronze_texto("cgccpfmecena") }} as cgccpfmecena,
    {{ bronze_timestamp("dtchegadarecibo") }} as dtchegadarecibo,
    {{ bronze_timestamp("dtrecibo") }} as dtrecibo,
    {{ bronze_numerico("captacaoreal") }} as captacaoreal,
    {{ bronze_texto("numerorecibo") }} as numerorecibo,
    {{ bronze_texto("tipoapoio") }} as tipoapoio,
    {{ bronze_texto("incentivador") }} as incentivador,
    {{ bronze_timestamp("dtliberacao") }} as dtliberacao,
    {{ bronze_texto("inabilitado") }} as inabilitado,
    {{ bronze_texto("certidao") }} as certidao,
    {{ bronze_texto("cadin") }} as cadin,
    {{ bronze_numerico("percentual") }} as percentual,
    {{ bronze_texto("idcaptacao") }} as idcaptacao,
    {{ bronze_texto("idpronac") }} as idpronac,
    {{ bronze_texto("situacao") }} as situacao,
    {{ bronze_texto("idsecretaria") }} as idsecretaria,
    _fatia
from {{ source("salic", "bronze_sac_vwpaineldetransferenciaderecursos") }}
