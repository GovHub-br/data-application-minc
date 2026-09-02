-- Bronze SALIC — sac__vwpagamentodespesaforaperiodoexecucao.
-- Origem: salic_bronze.sac__vwpagamentodespesaforaperiodoexecucao, onde tudo chega como
-- texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 12 colunas: 6 tipadas, 5 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_inteiro("pronac") }} as pronac,
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
from {{ source("bronze_sac", "sac__vwpagamentodespesaforaperiodoexecucao") }}
