-- Silver SALIC — silver_agentes_populacaomunicipio (ex-agentes__populacaomunicipio).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_agentes_populacaomunicipio, onde tudo chega como texto
-- da ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- População por município. Dois códigos IBGE: `idmunicipio` (6 dígitos) e
-- `idmunicipio7` (7 dígitos, com dígito verificador).
select
    {{ bronze_inteiro("idmunicipio") }} as idmunicipio,
    {{ bronze_inteiro("idmunicipio7") }} as idmunicipio7,
    {{ bronze_inteiro("populacao") }} as populacao,
    _fatia
from {{ source("salic", "bronze_agentes_populacaomunicipio") }}
