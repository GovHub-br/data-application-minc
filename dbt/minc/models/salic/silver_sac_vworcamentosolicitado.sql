-- Silver SALIC — silver_sac_vworcamentosolicitado (ex-sac__vworcamentosolicitado).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_vworcamentosolicitado, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 23 colunas: 8 tipadas, 14 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_texto("sequencial") }} as sequencial,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_inteiro("idproduto") }} as idproduto,
    {{ bronze_inteiro("idplanilhaproposta") }} as idplanilhaproposta,
    {{ bronze_texto("produto") }} as produto,
    {{ bronze_texto("etapa") }} as etapa,
    {{ bronze_texto("item") }} as item,
    {{ bronze_texto("unidade") }} as unidade,
    {{ bronze_numerico_cientifico("quantidade") }} as quantidade,
    {{ bronze_inteiro("ocorrencia") }} as ocorrencia,
    {{ bronze_numerico("valorunitario") }} as valorunitario,
    {{ bronze_numerico_cientifico("vltotal") }} as vltotal,
    {{ bronze_inteiro("qtdedias") }} as qtdedias,
    {{ bronze_texto("tipodespesa") }} as tipodespesa,
    {{ bronze_texto("tipopessoa") }} as tipopessoa,
    {{ bronze_texto("contrapartida") }} as contrapartida,
    {{ bronze_texto("fonterecurso") }} as fonterecurso,
    {{ bronze_texto("uf") }} as uf,
    {{ bronze_texto("municipio") }} as municipio,
    {{ bronze_texto("justificativa") }} as justificativa,
    _fatia
from {{ source("salic", "bronze_sac_vworcamentosolicitado") }}
