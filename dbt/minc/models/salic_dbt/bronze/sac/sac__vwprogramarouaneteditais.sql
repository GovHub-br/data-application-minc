-- Bronze SALIC — sac__vwprogramarouaneteditais.
-- Origem: salic_bronze.sac__vwprogramarouaneteditais, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 39 colunas: 14 tipadas, 24 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idpreprojeto") }} as idpreprojeto,
    {{ bronze_inteiro("idagente") }} as idagente,
    {{ bronze_texto("nmproponente") }} as nmproponente,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_texto("regiao") }} as regiao,
    {{ bronze_texto("uf") }} as uf,
    {{ bronze_texto("municipio") }} as municipio,
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_texto("cdarea") }} as cdarea,
    {{ bronze_texto("dsarea") }} as dsarea,
    {{ bronze_inteiro("cdsegmento") }} as cdsegmento,
    {{ bronze_texto("dssegmento") }} as dssegmento,
    {{ bronze_texto("cdsituacao") }} as cdsituacao,
    {{ bronze_texto("dssituacao") }} as dssituacao,
    {{ bronze_timestamp("dtinicioexecucao") }} as dtinicioexecucao,
    {{ bronze_timestamp("dtfimexecucao") }} as dtfimexecucao,
    {{ bronze_texto("tptipicidade") }} as tptipicidade,
    {{ bronze_inteiro("tptipologia") }} as tptipologia,
    {{ bronze_texto("dstipologia") }} as dstipologia,
    {{ bronze_texto("stestado") }} as stestado,
    {{ bronze_texto("idfase") }} as idfase,
    {{ bronze_texto("dsfase") }} as dsfase,
    {{ bronze_texto("dsestado") }} as dsestado,
    {{ bronze_inteiro("idprogramarouanet") }} as idprogramarouanet,
    {{ bronze_timestamp("dtabertura") }} as dtabertura,
    {{ bronze_timestamp("dtfechamento") }} as dtfechamento,
    {{ bronze_timestamp("dtresultado") }} as dtresultado,
    {{ bronze_texto("stestadoprograma") }} as stestadoprograma,
    {{ bronze_numerico("vltotalprograma") }} as vltotalprograma,
    {{ bronze_inteiro("nrpronac") }} as nrpronac,
    {{ bronze_texto("dslocalizacao") }} as dslocalizacao,
    {{ bronze_texto("vlproposta") }} as vlproposta,
    {{ bronze_numerico("vlaprovado") }} as vlaprovado,
    {{ bronze_texto("vlcaptado") }} as vlcaptado,
    {{ bronze_texto("dscontrato") }} as dscontrato,
    {{ bronze_texto("nrcnpjcpfincentivador") }} as nrcnpjcpfincentivador,
    {{ bronze_texto("nmincentivador") }} as nmincentivador,
    {{ bronze_texto("vlincentivo") }} as vlincentivo,
    _fatia
from {{ source("bronze_sac", "sac__vwprogramarouaneteditais") }}
