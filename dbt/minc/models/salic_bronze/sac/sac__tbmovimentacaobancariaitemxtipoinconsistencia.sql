-- Bronze SALIC — sac__tbmovimentacaobancariaitemxtipoinconsistencia.
-- Origem: salic_bronze.sac__tbmovimentacaobancariaitemxtipoinconsistencia, onde tudo
-- chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 3 colunas: 2 tipadas, 0 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idmovimentacaobancariaitem") }} as idmovimentacaobancariaitem,
    {{ bronze_inteiro("idtipoinconsistencia") }} as idtipoinconsistencia,
    _fatia
from {{ source("bronze_sac", "sac__tbmovimentacaobancariaitemxtipoinconsistencia") }}
