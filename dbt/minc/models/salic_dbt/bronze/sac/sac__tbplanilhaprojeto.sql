-- Bronze SALIC — sac__tbplanilhaprojeto.
-- Origem: salic_bronze.sac__tbplanilhaprojeto, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 24 colunas: 19 tipadas, 4 mantidas como texto.
-- 1 coluna(s) ficaram texto porque a amostra do banco
-- contradiz o tipo declarado no dicionário do SALIC.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idplanilhaprojeto") }} as idplanilhaprojeto,
    {{ bronze_inteiro("idplanilhaproposta") }} as idplanilhaproposta,
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_inteiro("idproduto") }} as idproduto,
    {{ bronze_inteiro("idetapa") }} as idetapa,
    {{ bronze_inteiro("idplanilhaitem") }} as idplanilhaitem,
    {{ bronze_texto("descricao") }} as descricao,
    {{ bronze_inteiro("idunidade") }} as idunidade,
    {{ bronze_texto("quantidade") }} as quantidade,
    {{ bronze_numerico("ocorrencia") }} as ocorrencia,
    {{ bronze_numerico("valorunitario") }} as valorunitario,
    {{ bronze_inteiro("qtdedias") }} as qtdedias,
    {{ bronze_inteiro("tipodespesa") }} as tipodespesa,
    {{ bronze_inteiro("tipopessoa") }} as tipopessoa,
    {{ bronze_inteiro("contrapartida") }} as contrapartida,
    {{ bronze_inteiro("fonterecurso") }} as fonterecurso,
    {{ bronze_inteiro("ufdespesa") }} as ufdespesa,
    {{ bronze_inteiro("municipiodespesa") }} as municipiodespesa,
    {{ bronze_texto("justificativa") }} as justificativa,
    {{ bronze_inteiro("idparecer") }} as idparecer,
    {{ bronze_inteiro("idusuario") }} as idusuario,
    {{ bronze_booleano("stcustopraticado") }} as stcustopraticado,
    {{ bronze_texto("tpacao") }} as tpacao,
    _fatia
from {{ source("bronze_sac", "sac__tbplanilhaprojeto") }}
