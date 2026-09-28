-- Bronze SALIC — sac__tbplanilhaaprovacao.
-- Origem: salic_bronze.sac__tbplanilhaaprovacao, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 30 colunas: 23 tipadas, 6 mantidas como texto.
-- 1 coluna(s) ficaram texto porque a amostra do banco
-- contradiz o tipo declarado no dicionário do SALIC.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idplanilhaaprovacao") }} as idplanilhaaprovacao,
    {{ bronze_texto("tpplanilha") }} as tpplanilha,
    {{ bronze_timestamp("dtplanilha") }} as dtplanilha,
    {{ bronze_inteiro("idplanilhaprojeto") }} as idplanilhaprojeto,
    {{ bronze_inteiro("idplanilhaproposta") }} as idplanilhaproposta,
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_inteiro("idproduto") }} as idproduto,
    {{ bronze_inteiro("idetapa") }} as idetapa,
    {{ bronze_inteiro("idplanilhaitem") }} as idplanilhaitem,
    {{ bronze_texto("dsitem") }} as dsitem,
    {{ bronze_inteiro("idunidade") }} as idunidade,
    {{ bronze_texto("qtitem") }} as qtitem,
    {{ bronze_numerico("nrocorrencia") }} as nrocorrencia,
    {{ bronze_numerico("vlunitario") }} as vlunitario,
    {{ bronze_inteiro("qtdias") }} as qtdias,
    {{ bronze_inteiro("tpdespesa") }} as tpdespesa,
    {{ bronze_inteiro("tppessoa") }} as tppessoa,
    {{ bronze_inteiro("nrcontrapartida") }} as nrcontrapartida,
    {{ bronze_inteiro("nrfonterecurso") }} as nrfonterecurso,
    {{ bronze_inteiro("idufdespesa") }} as idufdespesa,
    {{ bronze_inteiro("idmunicipiodespesa") }} as idmunicipiodespesa,
    {{ bronze_texto("dsjustificativa") }} as dsjustificativa,
    {{ bronze_inteiro("idagente") }} as idagente,
    {{ bronze_inteiro("idplanilhaaprovacaopai") }} as idplanilhaaprovacaopai,
    {{ bronze_inteiro("idreadequacao") }} as idreadequacao,
    {{ bronze_texto("tpacao") }} as tpacao,
    {{ bronze_inteiro("idrecursodecisao") }} as idrecursodecisao,
    {{ bronze_texto("stativo") }} as stativo,
    {{ bronze_booleano("stcustopraticado") }} as stcustopraticado,
    _fatia
from {{ source("bronze_sac", "sac__tbplanilhaaprovacao") }}
