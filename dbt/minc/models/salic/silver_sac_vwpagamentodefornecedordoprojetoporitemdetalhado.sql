-- Silver SALIC — silver_sac_vwpagamentodefornecedordoprojetoporitemdetalhado (ex-sac__vwpagamentodefornecedordoprojetoporitemdetalhado).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_vwpagamentodefornecedordoprojetoporitemdetalhado, onde tudo
-- chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 16 colunas: 5 tipadas, 10 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_texto("pronac") }} as pronac,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_texto("cnpjcpf") }} as cnpjcpf,
    {{ bronze_texto("fornecedor") }} as fornecedor,
    {{ bronze_texto("item") }} as item,
    {{ bronze_inteiro("idplanilhaitem") }} as idplanilhaitem,
    {{ bronze_texto("nrcomprovante") }} as nrcomprovante,
    {{ bronze_timestamp("dtemissao") }} as dtemissao,
    {{ bronze_texto("tpdocumento") }} as tpdocumento,
    {{ bronze_texto("dsjustificativa") }} as dsjustificativa,
    {{ bronze_timestamp("dtpagamento") }} as dtpagamento,
    {{ bronze_texto("tpformadepagamento") }} as tpformadepagamento,
    {{ bronze_texto("nrdocumentodepagamento") }} as nrdocumentodepagamento,
    {{ bronze_numerico("vlpago") }} as vlpago,
    _fatia
from {{ source("salic", "bronze_sac_vwpagamentodefornecedordoprojetoporitemdetalhado") }}
