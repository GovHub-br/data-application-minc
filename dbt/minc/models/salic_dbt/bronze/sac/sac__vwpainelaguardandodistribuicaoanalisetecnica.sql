-- Bronze SALIC — sac__vwpainelaguardandodistribuicaoanalisetecnica.
-- Origem: salic_bronze.sac__vwpainelaguardandodistribuicaoanalisetecnica, onde tudo
-- chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 21 colunas: 0 tipadas, 20 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("idpronac") }} as idpronac,
    {{ bronze_texto("nrprojeto") }} as nrprojeto,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_texto("idproduto") }} as idproduto,
    {{ bronze_texto("produto") }} as produto,
    {{ bronze_texto("stprincipal") }} as stprincipal,
    {{ bronze_texto("idarea") }} as idarea,
    {{ bronze_texto("area") }} as area,
    {{ bronze_texto("idsegmento") }} as idsegmento,
    {{ bronze_texto("segmento") }} as segmento,
    {{ bronze_texto("iddistribuirparecer") }} as iddistribuirparecer,
    {{ bronze_texto("idorgao") }} as idorgao,
    {{ bronze_texto("idorgaoorigem") }} as idorgaoorigem,
    {{ bronze_texto("fecharanalise") }} as fecharanalise,
    {{ bronze_texto("sianalise") }} as sianalise,
    {{ bronze_texto("siencaminhamento") }} as siencaminhamento,
    {{ bronze_texto("dtenviomincvinculada") }} as dtenviomincvinculada,
    {{ bronze_texto("qtdiasdistribuir") }} as qtdiasdistribuir,
    {{ bronze_texto("qtdesecundarios") }} as qtdesecundarios,
    {{ bronze_texto("valor") }} as valor,
    _fatia
from {{ source("bronze_sac", "sac__vwpainelaguardandodistribuicaoanalisetecnica") }}
