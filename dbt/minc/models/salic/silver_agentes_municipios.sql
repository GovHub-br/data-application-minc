-- Silver SALIC — silver_agentes_municipios (ex-agentes__municipios).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_agentes_municipios, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- Tabela de domínio geográfico: município IBGE, sua UF, meso e microrregião.
select
    {{ bronze_inteiro("idmunicipioibge") }} as idmunicipioibge,
    {{ bronze_inteiro("idufibge") }} as idufibge,
    {{ bronze_inteiro("idmeso") }} as idmeso,
    {{ bronze_inteiro("idmicro") }} as idmicro,
    {{ bronze_texto("descricao") }} as descricao,
    _fatia
from {{ source("salic", "bronze_agentes_municipios") }}
