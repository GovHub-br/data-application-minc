-- Silver SALIC — silver_sac_bancoagencianovo1 (ex-sac__bancoagencianovo1).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_bancoagencianovo1, onde tudo chega como texto da
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
from {{ source("salic", "bronze_sac_bancoagencianovo1") }}
