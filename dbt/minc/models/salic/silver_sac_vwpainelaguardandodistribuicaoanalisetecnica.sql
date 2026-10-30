-- Silver SALIC — silver_sac_vwpainelaguardandodistribuicaoanalisetecnica
-- (ex-sac__vwpainelaguardandodistribuicaoanalisetecnica).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_vwpainelaguardandodistribuicaoanalisetecnica, onde tudo
-- chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 21 colunas: 4 tipadas, 16 mantidas como texto.
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
    {{ bronze_timestamp("dtenviomincvinculada") }} as dtenviomincvinculada,
    {{ bronze_inteiro("qtdiasdistribuir") }} as qtdiasdistribuir,
    {{ bronze_inteiro("qtdesecundarios") }} as qtdesecundarios,
    {{ bronze_numerico_cientifico("valor") }} as valor,
    _fatia
from {{ source("salic", "bronze_sac_vwpainelaguardandodistribuicaoanalisetecnica") }}
