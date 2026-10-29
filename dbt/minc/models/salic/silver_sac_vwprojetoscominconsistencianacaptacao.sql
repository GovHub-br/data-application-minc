-- Silver SALIC — silver_sac_vwprojetoscominconsistencianacaptacao (ex-sac__vwprojetoscominconsistencianacaptacao).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_vwprojetoscominconsistencianacaptacao, onde tudo chega como
-- texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 24 colunas: 9 tipadas, 14 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("ano") }} as ano,
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_texto("pronac") }} as pronac,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_texto("cgccpf") }} as cgccpf,
    {{ bronze_texto("nmproponente") }} as nmproponente,
    {{ bronze_texto("situacao") }} as situacao,
    {{ bronze_timestamp("dtsituacao") }} as dtsituacao,
    {{ bronze_inteiro("orgao") }} as orgao,
    {{ bronze_texto("sigla") }} as sigla,
    {{ bronze_inteiro("idsecretaria") }} as idsecretaria,
    {{ bronze_texto("nrcpfcnpjproponente") }} as nrcpfcnpjproponente,
    {{ bronze_texto("nrcpfcnpjincentivador") }} as nrcpfcnpjincentivador,
    {{ bronze_texto("nmincentivador") }} as nmincentivador,
    {{ bronze_timestamp("dtcredito") }} as dtcredito,
    {{ bronze_numerico("vlvalorcredito") }} as vlvalorcredito,
    {{ bronze_inteiro("cdpatrocinio") }} as cdpatrocinio,
    {{ bronze_texto("nragenciaproponente") }} as nragenciaproponente,
    {{ bronze_texto("nrcontaproponente") }} as nrcontaproponente,
    {{ bronze_inteiro("tpvalidacao") }} as tpvalidacao,
    {{ bronze_texto("dstipoinconsistencia") }} as dstipoinconsistencia,
    {{ bronze_texto("dstipoincentivo") }} as dstipoincentivo,
    {{ bronze_numerico("vlsaldocontas") }} as vlsaldocontas,
    _fatia
from {{ source("salic", "bronze_sac_vwprojetoscominconsistencianacaptacao") }}
