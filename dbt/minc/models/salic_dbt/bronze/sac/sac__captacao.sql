-- Bronze SALIC — sac__captacao.
-- Origem: salic_bronze.sac__captacao, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 17 colunas: 8 tipadas, 8 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idcaptacao") }} as idcaptacao,
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_texto("sequencial") }} as sequencial,
    {{ bronze_texto("numerorecibo") }} as numerorecibo,
    {{ bronze_texto("cgccpfmecena") }} as cgccpfmecena,
    {{ bronze_texto("tipoapoio") }} as tipoapoio,
    {{ bronze_texto("medidaprovisoria") }} as medidaprovisoria,
    {{ bronze_timestamp("dtchegadarecibo") }} as dtchegadarecibo,
    {{ bronze_timestamp("dtrecibo") }} as dtrecibo,
    {{ bronze_numerico("captacaoreal") }} as captacaoreal,
    {{ bronze_numerico("captacaoufir") }} as captacaoufir,
    {{ bronze_inteiro("logon") }} as logon,
    {{ bronze_inteiro("idprojeto") }} as idprojeto,
    {{ bronze_texto("sitransferenciarecurso") }} as sitransferenciarecurso,
    {{ bronze_timestamp("dttransferenciarecurso") }} as dttransferenciarecurso,
    {{ bronze_texto("isbemservico") }} as isbemservico,
    _fatia
from {{ source("bronze_sac", "sac__captacao") }}
