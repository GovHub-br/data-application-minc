-- Bronze SALIC — sac__bancoagencia.
-- Origem: salic_bronze.sac__bancoagencia, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 14 colunas: 2 tipadas, 11 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("banco") }} as banco,
    {{ bronze_texto("agencia") }} as agencia,
    {{ bronze_texto("descricao") }} as descricao,
    {{ bronze_texto("endereco") }} as endereco,
    {{ bronze_texto("bairro") }} as bairro,
    {{ bronze_texto("cidade") }} as cidade,
    {{ bronze_texto("uf") }} as uf,
    {{ bronze_texto("cep") }} as cep,
    {{ bronze_texto("telefone") }} as telefone,
    {{ bronze_texto("fax") }} as fax,
    {{ bronze_inteiro("perfil") }} as perfil,
    {{ bronze_inteiro("id") }} as id,
    {{ bronze_texto("stestado") }} as stestado,
    _fatia
from {{ source("bronze_sac", "sac__bancoagencia") }}
