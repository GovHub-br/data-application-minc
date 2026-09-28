-- Bronze SALIC — agentes__populacaomunicipio.
-- Origem: salic_bronze.agentes__populacaomunicipio, onde tudo chega como texto
-- da ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- População por município. Dois códigos IBGE: `idmunicipio` (6 dígitos) e
-- `idmunicipio7` (7 dígitos, com dígito verificador).
select
    {{ bronze_inteiro("idmunicipio") }} as idmunicipio,
    {{ bronze_inteiro("idmunicipio7") }} as idmunicipio7,
    {{ bronze_inteiro("populacao") }} as populacao,
    _fatia
from {{ source("bronze_agentes", "agentes__populacaomunicipio") }}
