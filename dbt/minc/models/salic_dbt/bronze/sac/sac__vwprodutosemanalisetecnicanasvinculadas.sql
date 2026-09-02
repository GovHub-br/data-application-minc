-- Bronze SALIC — sac__vwprodutosemanalisetecnicanasvinculadas.
-- Origem: salic_bronze.sac__vwprodutosemanalisetecnicanasvinculadas, onde tudo chega
-- como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 37 colunas: 22 tipadas, 14 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_inteiro("cdorgaosuperior") }} as cdorgaosuperior,
    {{ bronze_inteiro("cdorgaoorigem") }} as cdorgaoorigem,
    {{ bronze_texto("sigla") }} as sigla,
    {{ bronze_inteiro("nrano") }} as nrano,
    {{ bronze_inteiro("nrpronac") }} as nrpronac,
    {{ bronze_texto("nmprojeto") }} as nmprojeto,
    {{ bronze_texto("cdsituacao") }} as cdsituacao,
    {{ bronze_texto("providenciatomada") }} as providenciatomada,
    {{ bronze_texto("parecer") }} as parecer,
    {{ bronze_texto("area") }} as area,
    {{ bronze_texto("datafixa") }} as datafixa,
    {{ bronze_inteiro("cdencaminhamento") }} as cdencaminhamento,
    {{ bronze_texto("dsencaminhamento") }} as dsencaminhamento,
    {{ bronze_texto("tpproduto") }} as tpproduto,
    {{ bronze_inteiro("cdanalise") }} as cdanalise,
    {{ bronze_inteiro("sifecharanalise") }} as sifecharanalise,
    {{ bronze_texto("tpanalise") }} as tpanalise,
    {{ bronze_inteiro("cdproduto") }} as cdproduto,
    {{ bronze_texto("dsanalise") }} as dsanalise,
    {{ bronze_timestamp("dtinicioexecucao") }} as dtinicioexecucao,
    {{ bronze_timestamp("dtfimexecucao") }} as dtfimexecucao,
    {{ bronze_timestamp("dtprimeiroenvio") }} as dtprimeiroenvio,
    {{ bronze_timestamp("dtultimoenvio") }} as dtultimoenvio,
    {{ bronze_timestamp("dtdistribuicao") }} as dtdistribuicao,
    {{ bronze_texto("nrcpf_parecerista") }} as nrcpf_parecerista,
    {{ bronze_texto("parecerista") }} as parecerista,
    {{ bronze_texto("tpavaliador") }} as tpavaliador,
    {{ bronze_timestamp("dtdevolucao") }} as dtdevolucao,
    {{ bronze_inteiro("mesdoultimoenvio") }} as mesdoultimoenvio,
    {{ bronze_inteiro("nrdiastotalcomparecerista") }} as nrdiastotalcomparecerista,
    {{ bronze_inteiro("nrdiasdeanalisedoparecerita") }} as nrdiasdeanalisedoparecerita,
    {{ bronze_inteiro("nrdiasemdilgenciapeloparecerista") }}
    as nrdiasemdilgenciapeloparecerista,
    {{ bronze_inteiro("qtdediasnavinculada") }} as qtdediasnavinculada,
    {{ bronze_numerico("vlsolicitado") }} as vlsolicitado,
    {{ bronze_numerico("vlsugerido") }} as vlsugerido,
    _fatia
from {{ source("bronze_sac", "sac__vwprodutosemanalisetecnicanasvinculadas") }}
