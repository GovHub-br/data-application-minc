-- Bronze SALIC — sac__vwpainelemvalidacao.
-- Origem: salic_bronze.sac__vwpainelemvalidacao, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 31 colunas: 15 tipadas, 15 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_inteiro("nrprojeto") }} as nrprojeto,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_inteiro("idproduto") }} as idproduto,
    {{ bronze_texto("produto") }} as produto,
    {{ bronze_inteiro("idarea") }} as idarea,
    {{ bronze_texto("area") }} as area,
    {{ bronze_texto("idsegmento") }} as idsegmento,
    {{ bronze_texto("segmento") }} as segmento,
    {{ bronze_texto("tipoanalise") }} as tipoanalise,
    {{ bronze_inteiro("iddistribuirparecer") }} as iddistribuirparecer,
    {{ bronze_inteiro("idagenteparecerista") }} as idagenteparecerista,
    {{ bronze_texto("parecerista") }} as parecerista,
    {{ bronze_inteiro("idorgao") }} as idorgao,
    {{ bronze_inteiro("idorgaoorigem") }} as idorgaoorigem,
    {{ bronze_texto("sianalise") }} as sianalise,
    {{ bronze_texto("siencaminhamento") }} as siencaminhamento,
    {{ bronze_timestamp("dtenviomincvinculada") }} as dtenviomincvinculada,
    {{ bronze_timestamp("dtdistribuicao") }} as dtdistribuicao,
    {{ bronze_timestamp("dtdevolucao") }} as dtdevolucao,
    {{ bronze_inteiro("tempototalanalise") }} as tempototalanalise,
    {{ bronze_inteiro("tempoparecerista") }} as tempoparecerista,
    {{ bronze_inteiro("tempodiligencia") }} as tempodiligencia,
    {{ bronze_texto("qtdiligenciaproduto") }} as qtdiligenciaproduto,
    {{ bronze_texto("valor") }} as valor,
    {{ bronze_texto("obs") }} as obs,
    {{ bronze_texto("tecnicovalidador") }} as tecnicovalidador,
    {{ bronze_timestamp("dtvalidacao") }} as dtvalidacao,
    {{ bronze_texto("stprincipal") }} as stprincipal,
    {{ bronze_texto("fecharanalise") }} as fecharanalise,
    _fatia
from {{ source("bronze_sac", "sac__vwpainelemvalidacao") }}
