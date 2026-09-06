-- Bronze SALIC — sac__tbdistribuirprojetopassivo.
-- Origem: salic_bronze.sac__tbdistribuirprojetopassivo, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 8 colunas: 6 tipadas, 1 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("iddistribuirprojetopassivo") }} as iddistribuirprojetopassivo,
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_inteiro("idtecnico") }} as idtecnico,
    {{ bronze_timestamp("dtrecebimento") }} as dtrecebimento,
    {{ bronze_timestamp("dtrestituicao") }} as dtrestituicao,
    {{ bronze_texto("dsjustificativa") }} as dsjustificativa,
    {{ bronze_booleano("stestado") }} as stestado,
    _fatia
from {{ source("bronze_sac", "sac__tbdistribuirprojetopassivo") }}
