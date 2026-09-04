-- Bronze SALIC — agentes__municipios.
-- Origem: salic_bronze.agentes__municipios, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 6 colunas: 1 tipada, 5 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
--
-- Os códigos ficam como texto de propósito. `idmunicipioibge` tem 6 posições
-- em todas as 5.568 linhas — é o código IBGE sem o dígito verificador — e
-- convertê-lo em número perderia zero à esquerda em qualquer série futura.
select
    {{ bronze_texto("idmunicipioibge") }} as idmunicipioibge,
    {{ bronze_inteiro("idufibge") }} as idufibge,
    {{ bronze_texto("idmeso") }} as idmeso,
    {{ bronze_texto("idmicro") }} as idmicro,
    {{ bronze_texto("descricao") }} as descricao,
    _fatia
from {{ source("bronze_agentes", "agentes__municipios") }}
