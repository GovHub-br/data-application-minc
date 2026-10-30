-- Silver SALIC — silver_sac_tbmanterportaria (ex-sac__tbmanterportaria).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_tbmanterportaria, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 7 colunas: 3 tipadas, 3 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idmanterportaria") }} as idmanterportaria,
    {{ bronze_texto("dsassinante") }} as dsassinante,
    {{ bronze_timestamp("dtportariapublicacao") }} as dtportariapublicacao,
    {{ bronze_booleano("stestado") }} as stestado,
    {{ bronze_texto("dscargo") }} as dscargo,
    {{ bronze_texto("dsportaria") }} as dsportaria,
    _fatia
from {{ source("salic", "bronze_sac_tbmanterportaria") }}
