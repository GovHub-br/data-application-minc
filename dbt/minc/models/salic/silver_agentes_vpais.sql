-- Silver SALIC — silver_agentes_vpais (ex-agentes__vpais).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_agentes_vpais, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 5 colunas: 1 tipadas, 3 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idpais") }} as idpais,
    {{ bronze_texto("sigla") }} as sigla,
    {{ bronze_texto("descricao") }} as descricao,
    {{ bronze_texto("continente") }} as continente,
    _fatia
from {{ source("salic", "bronze_agentes_vpais") }}
