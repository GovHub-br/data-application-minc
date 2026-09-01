-- Bronze SALIC — sac__vwpainelprojetossgptc.
-- Origem: salic_bronze.sac__vwpainelprojetossgptc, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 31 colunas: 23 tipadas, 7 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_inteiro("nrpronac") }} as nrpronac,
    {{ bronze_inteiro("anoprojeto") }} as anoprojeto,
    {{ bronze_inteiro("sequencial") }} as sequencial,
    {{ bronze_texto("nmprojeto") }} as nmprojeto,
    {{ bronze_texto("proponente") }} as proponente,
    {{ bronze_inteiro("cdarea") }} as cdarea,
    {{ bronze_texto("cdsegmento") }} as cdsegmento,
    {{ bronze_texto("cdsituacao") }} as cdsituacao,
    {{ bronze_timestamp("dtsituacao") }} as dtsituacao,
    {{ bronze_texto("cdultimasituacaoprescricao") }} as cdultimasituacaoprescricao,
    {{ bronze_timestamp("dtultimasituacaoprescricao") }} as dtultimasituacaoprescricao,
    {{ bronze_timestamp("dtinicioexecucao") }} as dtinicioexecucao,
    {{ bronze_timestamp("dtfimexecucao") }} as dtfimexecucao,
    {{ bronze_timestamp("dtfimcaptacao") }} as dtfimcaptacao,
    {{ bronze_inteiro("idsecretaria") }} as idsecretaria,
    {{ bronze_inteiro("idorgao") }} as idorgao,
    {{ bronze_texto("cdufprojeto") }} as cdufprojeto,
    {{ bronze_numerico("vlsolicitado") }} as vlsolicitado,
    {{ bronze_numerico("vlaprovado") }} as vlaprovado,
    {{ bronze_numerico("vlcaptado") }} as vlcaptado,
    {{ bronze_numerico("vltransferidomovimentacao") }} as vltransferidomovimentacao,
    {{ bronze_numerico("vlperccaptado") }} as vlperccaptado,
    {{ bronze_numerico("vlcomprovado") }} as vlcomprovado,
    {{ bronze_numerico("vlpercomprovado") }} as vlpercomprovado,
    {{ bronze_texto("vlsaldocontacaptacao") }} as vlsaldocontacaptacao,
    {{ bronze_numerico("vlsaldocontamovimento") }} as vlsaldocontamovimento,
    {{ bronze_numerico("vlsaldodascontas") }} as vlsaldodascontas,
    {{ bronze_numerico("vlutilizado") }} as vlutilizado,
    {{ bronze_numerico("vlpercutilizado") }} as vlpercutilizado,
    _fatia
from {{ source("bronze_sac", "sac__vwpainelprojetossgptc") }}
