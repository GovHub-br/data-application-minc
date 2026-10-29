-- Silver SALIC — silver_sac_vwplanilhaaprovada (ex-sac__vwplanilhaaprovada).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_vwplanilhaaprovada, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 27 colunas: 10 tipadas, 16 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_texto("pronac") }} as pronac,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_inteiro("idproduto") }} as idproduto,
    {{ bronze_inteiro("idplanilhaaprovacao") }} as idplanilhaaprovacao,
    {{ bronze_texto("produto") }} as produto,
    {{ bronze_texto("etapa") }} as etapa,
    {{ bronze_texto("item") }} as item,
    {{ bronze_numerico_cientifico("vlsolicitado") }} as vlsolicitado,
    {{ bronze_texto("justproponente") }} as justproponente,
    {{ bronze_numerico_cientifico("vlsugerido") }} as vlsugerido,
    {{ bronze_texto("justparecerista") }} as justparecerista,
    {{ bronze_texto("unidade") }} as unidade,
    {{ bronze_numerico_cientifico("qtitem") }} as qtitem,
    {{ bronze_inteiro("nrocorrencia") }} as nrocorrencia,
    {{ bronze_numerico("vlunitario") }} as vlunitario,
    {{ bronze_inteiro("qtdias") }} as qtdias,
    {{ bronze_texto("tpdespesa") }} as tpdespesa,
    {{ bronze_texto("tppessoa") }} as tppessoa,
    {{ bronze_texto("nrcontrapartida") }} as nrcontrapartida,
    {{ bronze_texto("idfonte") }} as idfonte,
    {{ bronze_texto("fonterecurso") }} as fonterecurso,
    {{ bronze_texto("uf") }} as uf,
    {{ bronze_texto("municipio") }} as municipio,
    {{ bronze_numerico_cientifico("aprovado") }} as aprovado,
    {{ bronze_texto("justcomponente") }} as justcomponente,
    _fatia
from {{ source("salic", "bronze_sac_vwplanilhaaprovada") }}
