-- Silver SALIC — silver_sac_vwpagamentodespesaforaperiodoexecucao
-- (ex-sac__vwpagamentodespesaforaperiodoexecucao).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_vwpagamentodespesaforaperiodoexecucao, onde tudo chega como
-- texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 12 colunas: 5 tipadas, 6 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_texto("pronac") }} as pronac,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_texto("item") }} as item,
    {{ bronze_texto("fornecedor") }} as fornecedor,
    {{ bronze_timestamp("dtinicioexecucao") }} as dtinicioexecucao,
    {{ bronze_timestamp("dtfimexecucao") }} as dtfimexecucao,
    {{ bronze_texto("nrcomprovante") }} as nrcomprovante,
    {{ bronze_timestamp("dtpagamento") }} as dtpagamento,
    {{ bronze_numerico("vlcomprovado") }} as vlcomprovado,
    {{ bronze_texto("obs") }} as obs,
    _fatia
from {{ source("salic", "bronze_sac_vwpagamentodespesaforaperiodoexecucao") }}
