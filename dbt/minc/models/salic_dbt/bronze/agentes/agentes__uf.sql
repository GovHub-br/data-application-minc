-- Bronze SALIC — agentes__uf.
-- Origem: salic_bronze.agentes__uf, onde tudo chega como texto da ingestão via
-- Trino (ADR 0005). Tipar é o trabalho desta camada.
-- Tabela de domínio: as 27 unidades da federação, com sigla, nome e região.
select
    {{ bronze_inteiro("iduf") }} as iduf,
    {{ bronze_texto("sigla") }} as sigla,
    {{ bronze_texto("descricao") }} as descricao,
    {{ bronze_texto("regiao") }} as regiao,
    _fatia
from {{ source("bronze_agentes", "agentes__uf") }}
