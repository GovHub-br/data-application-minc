-- Silver SALIC — silver_sac_vwpagamentodefornecedordoprojetoporitem
-- (ex-sac__vwpagamentodefornecedordoprojetoporitem).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_vwpagamentodefornecedordoprojetoporitem, onde tudo chega
-- como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 8 colunas: 2 tipadas, 5 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_texto("pronac") }} as pronac,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_texto("cnpjcpf") }} as cnpjcpf,
    {{ bronze_texto("fornecedor") }} as fornecedor,
    {{ bronze_texto("item") }} as item,
    {{ bronze_numerico("vlpago") }} as vlpago,
    _fatia
from {{ source("salic", "bronze_sac_vwpagamentodefornecedordoprojetoporitem") }}
