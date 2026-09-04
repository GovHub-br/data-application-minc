-- Bronze SALIC — agentes__verificacao.
-- Origem: salic_bronze.agentes__verificacao, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- Tabela de domínio: os valores possíveis de "verificação" de um agente
-- (natureza jurídica, esfera etc.), agrupados por idtipo.
select
    {{ bronze_inteiro("idverificacao") }} as idverificacao,
    {{ bronze_inteiro("idtipo") }} as idtipo,
    {{ bronze_texto("descricao") }} as descricao,
    {{ bronze_inteiro("sistema") }} as sistema,
    _fatia
from {{ source("bronze_agentes", "agentes__verificacao") }}
