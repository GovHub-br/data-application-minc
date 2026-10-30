-- Silver SALIC — silver_sac_tbprioridadedeanalise (ex-sac__tbprioridadedeanalise).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_tbprioridadedeanalise, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 10 colunas: 8 tipadas, 1 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idprioridadedeanalise") }} as idprioridadedeanalise,
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_timestamp("dtprioridade") }} as dtprioridade,
    {{ bronze_texto("dsmotivacao") }} as dsmotivacao,
    {{ bronze_inteiro("idnrreuniao") }} as idnrreuniao,
    {{ bronze_booleano("siurgencia") }} as siurgencia,
    {{ bronze_booleano("tpanalise") }} as tpanalise,
    {{ bronze_booleano("stestado") }} as stestado,
    {{ bronze_inteiro("idusuario") }} as idusuario,
    _fatia
from {{ source("salic", "bronze_sac_tbprioridadedeanalise") }}
