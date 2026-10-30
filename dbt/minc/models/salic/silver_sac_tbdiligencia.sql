-- Silver SALIC — silver_sac_tbdiligencia (ex-sac__tbdiligencia).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_tbdiligencia, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 18 colunas: 12 tipadas, 5 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("iddiligencia") }} as iddiligencia,
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_inteiro("idtipodiligencia") }} as idtipodiligencia,
    {{ bronze_timestamp("dtsolicitacao") }} as dtsolicitacao,
    {{ bronze_texto("solicitacao") }} as solicitacao,
    {{ bronze_inteiro("idsolicitante") }} as idsolicitante,
    {{ bronze_timestamp("dtresposta") }} as dtresposta,
    {{ bronze_texto("resposta") }} as resposta,
    {{ bronze_inteiro("idproponente") }} as idproponente,
    {{ bronze_booleano("stestado") }} as stestado,
    {{ bronze_inteiro("idplanodistribuicao") }} as idplanodistribuicao,
    {{ bronze_inteiro("idarquivo") }} as idarquivo,
    {{ bronze_inteiro("idcodigodocumentosexigidos") }} as idcodigodocumentosexigidos,
    {{ bronze_inteiro("idproduto") }} as idproduto,
    {{ bronze_texto("stprorrogacao") }} as stprorrogacao,
    {{ bronze_texto("stenviado") }} as stenviado,
    {{ bronze_texto("cdsituacaoorigem") }} as cdsituacaoorigem,
    _fatia
from {{ source("salic", "bronze_sac_tbdiligencia") }}
