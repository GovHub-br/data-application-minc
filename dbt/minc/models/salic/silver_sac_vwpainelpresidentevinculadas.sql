-- Silver SALIC — silver_sac_vwpainelpresidentevinculadas (ex-sac__vwpainelpresidentevinculadas).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_vwpainelpresidentevinculadas, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 18 colunas: 7 tipadas, 10 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_inteiro("nrprojeto") }} as nrprojeto,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_inteiro("idarea") }} as idarea,
    {{ bronze_texto("area") }} as area,
    {{ bronze_texto("idsegmento") }} as idsegmento,
    {{ bronze_texto("segmento") }} as segmento,
    {{ bronze_inteiro("idproduto") }} as idproduto,
    {{ bronze_inteiro("iddistribuirparecer") }} as iddistribuirparecer,
    {{ bronze_texto("parecerista") }} as parecerista,
    {{ bronze_inteiro("idorgao") }} as idorgao,
    {{ bronze_texto("tipoanalise") }} as tipoanalise,
    {{ bronze_texto("stprincipal") }} as stprincipal,
    {{ bronze_texto("fecharanalise") }} as fecharanalise,
    {{ bronze_texto("tecnicovalidador") }} as tecnicovalidador,
    {{ bronze_timestamp("dtvalidacao") }} as dtvalidacao,
    {{ bronze_texto("valor") }} as valor,
    _fatia
from {{ source("salic", "bronze_sac_vwpainelpresidentevinculadas") }}
