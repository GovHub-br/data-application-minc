-- Bronze SALIC — sac__vwanalisedecusto.
-- Origem: salic_bronze.sac__vwanalisedecusto, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 27 colunas: 9 tipadas, 17 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_texto("sequencial") }} as sequencial,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_inteiro("idproduto") }} as idproduto,
    {{ bronze_inteiro("idplanilhaprojeto") }} as idplanilhaprojeto,
    {{ bronze_texto("produto") }} as produto,
    {{ bronze_texto("etapa") }} as etapa,
    {{ bronze_texto("item") }} as item,
    {{ bronze_texto("unidade") }} as unidade,
    {{ bronze_texto("quantidade") }} as quantidade,
    {{ bronze_inteiro("ocorrencia") }} as ocorrencia,
    {{ bronze_numerico("valorunitario") }} as valorunitario,
    {{ bronze_texto("vltotal") }} as vltotal,
    {{ bronze_inteiro("qtdedias") }} as qtdedias,
    {{ bronze_texto("tipodespesa") }} as tipodespesa,
    {{ bronze_texto("tipopessoa") }} as tipopessoa,
    {{ bronze_texto("contrapartida") }} as contrapartida,
    {{ bronze_texto("fonterecurso") }} as fonterecurso,
    {{ bronze_texto("uf") }} as uf,
    {{ bronze_texto("municipio") }} as municipio,
    {{ bronze_timestamp("data") }} as data,
    {{ bronze_numerico("vlcorte") }} as vlcorte,
    {{ bronze_texto("justificativasugerida") }} as justificativasugerida,
    {{ bronze_texto("vlsugerido") }} as vlsugerido,
    {{ bronze_inteiro("idusuario") }} as idusuario,
    _fatia
from {{ source("bronze_sac", "sac__vwanalisedecusto") }}
