-- Bronze SALIC — sac__vwplanilhasugeridacompleta.
-- Origem: salic_bronze.sac__vwplanilhasugeridacompleta, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 25 colunas: 8 tipadas, 16 mantidas como texto.
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
    {{ bronze_texto("vlsolicitado") }} as vlsolicitado,
    {{ bronze_texto("unidade") }} as unidade,
    {{ bronze_texto("quantidade") }} as quantidade,
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
    {{ bronze_texto("sugerido") }} as sugerido,
    {{ bronze_texto("justificativa") }} as justificativa,
    {{ bronze_inteiro("idusuario") }} as idusuario,
    _fatia
from {{ source("bronze_sac", "sac__vwplanilhasugeridacompleta") }}
