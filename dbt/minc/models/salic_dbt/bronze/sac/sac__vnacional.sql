-- Bronze SALIC — sac__vnacional.
-- Origem: salic_bronze.sac__vnacional, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 15 colunas: 9 tipadas, 5 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idendereco") }} as idendereco,
    {{ bronze_inteiro("idagente") }} as idagente,
    {{ bronze_inteiro("tipoendereco") }} as tipoendereco,
    {{ bronze_inteiro("tipologradouro") }} as tipologradouro,
    {{ bronze_texto("logradouro") }} as logradouro,
    {{ bronze_inteiro("numero") }} as numero,
    {{ bronze_texto("bairro") }} as bairro,
    {{ bronze_texto("complemento") }} as complemento,
    {{ bronze_inteiro("cidade") }} as cidade,
    {{ bronze_inteiro("uf") }} as uf,
    {{ bronze_inteiro("cep") }} as cep,
    {{ bronze_texto("status") }} as status,
    {{ bronze_texto("divulgar") }} as divulgar,
    {{ bronze_inteiro("usuario") }} as usuario,
    _fatia
from {{ source("bronze_sac", "sac__vnacional") }}
