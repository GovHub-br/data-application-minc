-- Silver SALIC — silver_sac_kitentregue (ex-sac__kitentregue).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_kitentregue, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 8 colunas: 4 tipadas, 3 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_texto("sequencial") }} as sequencial,
    {{ bronze_timestamp("dtaprovacao") }} as dtaprovacao,
    {{ bronze_texto("documento") }} as documento,
    {{ bronze_timestamp("dtentrega") }} as dtentrega,
    {{ bronze_numerico("valorkit") }} as valorkit,
    {{ bronze_inteiro("logon") }} as logon,
    _fatia
from {{ source("salic", "bronze_sac_kitentregue") }}
