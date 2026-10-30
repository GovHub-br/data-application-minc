-- Silver SALIC — silver_agentes_verificacao (ex-agentes__verificacao).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_agentes_verificacao, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- Tabela de domínio: os valores possíveis de "verificação" de um agente
-- (natureza jurídica, esfera etc.), agrupados por idtipo.
select
    {{ bronze_inteiro("idverificacao") }} as idverificacao,
    {{ bronze_inteiro("idtipo") }} as idtipo,
    {{ bronze_texto("descricao") }} as descricao,
    {{ bronze_inteiro("sistema") }} as sistema,
    _fatia
from {{ source("salic", "bronze_agentes_verificacao") }}
