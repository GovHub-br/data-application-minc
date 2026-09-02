-- Bronze SALIC — sac__tbprojetofase.
-- Origem: salic_bronze.sac__tbprojetofase, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 8 colunas: 7 tipadas, 0 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idprojetofase") }} as idprojetofase,
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_inteiro("idnormativo") }} as idnormativo,
    {{ bronze_inteiro("idfase") }} as idfase,
    {{ bronze_timestamp("dtiniciofase") }} as dtiniciofase,
    {{ bronze_timestamp("dtfinalfase") }} as dtfinalfase,
    {{ bronze_booleano("stestado") }} as stestado,
    _fatia
from {{ source("bronze_sac", "sac__tbprojetofase") }}
