-- Bronze SALIC — sac__bancoagencianovo1.
-- Origem: salic_bronze.sac__bancoagencianovo1, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 9 colunas: 0 tipadas, 8 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("banco") }} as banco,
    {{ bronze_texto("agencia") }} as agencia,
    {{ bronze_texto("descricao") }} as descricao,
    {{ bronze_texto("cidade") }} as cidade,
    {{ bronze_texto("endereco") }} as endereco,
    {{ bronze_texto("cep") }} as cep,
    {{ bronze_texto("uf") }} as uf,
    {{ bronze_texto("perfil") }} as perfil,
    _fatia
from {{ source("bronze_sac", "sac__bancoagencianovo1") }}
