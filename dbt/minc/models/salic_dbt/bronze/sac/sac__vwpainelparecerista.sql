-- Bronze SALIC — sac__vwpainelparecerista.
-- Origem: salic_bronze.sac__vwpainelparecerista, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 32 colunas: 17 tipadas, 14 mantidas como texto.
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
    {{ bronze_inteiro("iddistribuirparecer") }} as iddistribuirparecer,
    {{ bronze_inteiro("idorgao") }} as idorgao,
    {{ bronze_timestamp("dtenviomincvinculada") }} as dtenviomincvinculada,
    {{ bronze_timestamp("dtdistribuicao") }} as dtdistribuicao,
    {{ bronze_inteiro("idorgaoorigem") }} as idorgaoorigem,
    {{ bronze_texto("sianalise") }} as sianalise,
    {{ bronze_texto("siencaminhamento") }} as siencaminhamento,
    {{ bronze_inteiro("qtdiasparadistribuir") }} as qtdiasparadistribuir,
    {{ bronze_texto("dtdevolucao") }} as dtdevolucao,
    {{ bronze_inteiro("tempototalanalise") }} as tempototalanalise,
    {{ bronze_inteiro("tempoparecerista") }} as tempoparecerista,
    {{ bronze_inteiro("tempodiligencia") }} as tempodiligencia,
    {{ bronze_inteiro("idagenteparecerista") }} as idagenteparecerista,
    {{ bronze_texto("parecerista") }} as parecerista,
    {{ bronze_inteiro("qtdesecundarios") }} as qtdesecundarios,
    {{ bronze_timestamp("dtenviodiligencia") }} as dtenviodiligencia,
    {{ bronze_timestamp("dtrespostadiligencia") }} as dtrespostadiligencia,
    {{ bronze_texto("qtdiligenciaproduto") }} as qtdiligenciaproduto,
    {{ bronze_texto("valor") }} as valor,
    {{ bronze_texto("observacao") }} as observacao,
    {{ bronze_texto("stprincipal") }} as stprincipal,
    {{ bronze_texto("fecharanalise") }} as fecharanalise,
    _fatia
from {{ source("bronze_sac", "sac__vwpainelparecerista") }}
