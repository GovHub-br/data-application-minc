-- Silver SALIC — silver_sac_vwplanilhasugeridacompleta
-- (ex-sac__vwplanilhasugeridacompleta).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_vwplanilhasugeridacompleta, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 25 colunas: 11 tipadas, 13 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_texto("pronac") }} as pronac,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_inteiro("idproduto") }} as idproduto,
    {{ bronze_inteiro("idplanilhaprojeto") }} as idplanilhaprojeto,
    {{ bronze_texto("produto") }} as produto,
    {{ bronze_texto("etapa") }} as etapa,
    {{ bronze_texto("item") }} as item,
    {{ bronze_numerico_cientifico("vlsolicitado") }} as vlsolicitado,
    {{ bronze_texto("unidade") }} as unidade,
    {{ bronze_numerico_cientifico("quantidade") }} as quantidade,
    {{ bronze_inteiro("ocorrencia") }} as ocorrencia,
    {{ bronze_numerico("valorunitario") }} as valorunitario,
    {{ bronze_inteiro("qtdedias") }} as qtdedias,
    {{ bronze_texto("tipodespesa") }} as tipodespesa,
    {{ bronze_texto("tipopessoa") }} as tipopessoa,
    {{ bronze_texto("contrapartida") }} as contrapartida,
    {{ bronze_inteiro("idfonte") }} as idfonte,
    {{ bronze_texto("fonterecurso") }} as fonterecurso,
    {{ bronze_texto("uf") }} as uf,
    {{ bronze_texto("municipio") }} as municipio,
    {{ bronze_numerico_cientifico("sugerido") }} as sugerido,
    {{ bronze_texto("justificativa") }} as justificativa,
    {{ bronze_inteiro("idusuario") }} as idusuario,
    _fatia
from {{ source("salic", "bronze_sac_vwplanilhasugeridacompleta") }}
