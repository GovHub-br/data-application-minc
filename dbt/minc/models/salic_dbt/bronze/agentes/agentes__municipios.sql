-- Bronze SALIC — agentes__municipios.
-- Origem: salic_bronze.agentes__municipios, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- Tabela de domínio geográfico: município IBGE, sua UF, meso e microrregião.
select
    {{ bronze_inteiro("idmunicipioibge") }} as idmunicipioibge,
    {{ bronze_inteiro("idufibge") }} as idufibge,
    {{ bronze_inteiro("idmeso") }} as idmeso,
    {{ bronze_inteiro("idmicro") }} as idmicro,
    {{ bronze_texto("descricao") }} as descricao,
    _fatia
from {{ source("bronze_agentes", "agentes__municipios") }}
