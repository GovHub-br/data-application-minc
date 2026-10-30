-- Silver SALIC — silver_sac_vproponenteavaliado (ex-sac__vproponenteavaliado).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_vproponenteavaliado, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 5 colunas: 1 tipadas, 3 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("cgccpf") }} as cgccpf,
    {{ bronze_texto("proponente") }} as proponente,
    {{ bronze_texto("classificacao") }} as classificacao,
    {{ bronze_inteiro("peso") }} as peso,
    _fatia
from {{ source("salic", "bronze_sac_vproponenteavaliado") }}
