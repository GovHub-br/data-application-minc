-- Bronze SALIC — agentes__endereconacional.
-- Origem: salic_bronze.agentes__endereconacional, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- Uma linha por endereço nacional de agente. DADO PESSOAL (endereço).
-- `cep` fica TEXT (zero à esquerda); `cidade` é o código IBGE do município.
select
    {{ bronze_inteiro("idendereco") }} as idendereco,
    {{ bronze_inteiro("idagente") }} as idagente,
    {{ bronze_inteiro("tipoendereco") }} as tipoendereco,
    {{ bronze_inteiro("tipologradouro") }} as tipologradouro,
    {{ bronze_texto("logradouro") }} as logradouro,
    {{ bronze_texto("numero") }} as numero,
    {{ bronze_texto("bairro") }} as bairro,
    {{ bronze_texto("complemento") }} as complemento,
    {{ bronze_inteiro("cidade") }} as cidade,
    {{ bronze_inteiro("uf") }} as uf,
    {{ bronze_texto("cep") }} as cep,
    {{ bronze_texto("municipio") }} as municipio,
    {{ bronze_texto("ufdescricao") }} as ufdescricao,
    {{ bronze_booleano("status") }} as status,
    {{ bronze_booleano("divulgar") }} as divulgar,
    {{ bronze_inteiro("usuario") }} as usuario,
    _fatia
from {{ source("bronze_agentes", "agentes__endereconacional") }}
