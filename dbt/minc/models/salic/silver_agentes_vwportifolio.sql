-- Silver SALIC — silver_agentes_vwportifolio (ex-agentes__vwportifolio).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_agentes_vwportifolio, onde tudo chega como texto da
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
from {{ source("salic", "bronze_agentes_vwportifolio") }}
