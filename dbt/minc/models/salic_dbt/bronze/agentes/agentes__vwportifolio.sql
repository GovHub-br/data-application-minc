-- Bronze SALIC — agentes__vwportifolio.
-- Origem: salic_bronze.agentes__vwportifolio, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 6 colunas: 2 tipadas, 3 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("cnpjcpf", tipo="bigint") }} as cnpjcpf,
    {{ bronze_texto("proponente") }} as proponente,
    {{ bronze_timestamp("dtportifolio") }} as dtportifolio,
    {{ bronze_texto("siportifolio") }} as siportifolio,
    {{ bronze_texto("responsavel") }} as responsavel,
    _fatia
from {{ source("bronze_agentes", "agentes__vwportifolio") }}
