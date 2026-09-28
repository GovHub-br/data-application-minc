-- Bronze SALIC — sac__tblancamentobancario.
-- Origem: salic_bronze.sac__tblancamentobancario, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 13 colunas: 6 tipadas, 6 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idlancamentobancario") }} as idlancamentobancario,
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_inteiro("idmovimentacaobancariaitem") }} as idmovimentacaobancariaitem,
    {{ bronze_booleano("stcontalancamento") }} as stcontalancamento,
    {{ bronze_texto("nragencialancamento") }} as nragencialancamento,
    {{ bronze_texto("nrcontalancamento") }} as nrcontalancamento,
    {{ bronze_texto("cdlancamento") }} as cdlancamento,
    {{ bronze_texto("dslancamento") }} as dslancamento,
    {{ bronze_texto("nrlancamento") }} as nrlancamento,
    {{ bronze_timestamp("dtlancamento") }} as dtlancamento,
    {{ bronze_numerico("vllancamento") }} as vllancamento,
    {{ bronze_texto("stlancamento") }} as stlancamento,
    _fatia
from {{ source("bronze_sac", "sac__tblancamentobancario") }}
