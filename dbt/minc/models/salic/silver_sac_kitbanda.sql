-- Silver SALIC — silver_sac_kitbanda (ex-sac__kitbanda).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_kitbanda, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 9 colunas: 3 tipadas, 5 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("codigo") }} as codigo,
    {{ bronze_texto("descricao") }} as descricao,
    {{ bronze_inteiro("quantidade") }} as quantidade,
    {{ bronze_texto("kit") }} as kit,
    {{ bronze_texto("especificacao") }} as especificacao,
    {{ bronze_texto("afinacao") }} as afinacao,
    {{ bronze_texto("acabamento") }} as acabamento,
    {{ bronze_numerico("precounitario") }} as precounitario,
    _fatia
from {{ source("salic", "bronze_sac_kitbanda") }}
