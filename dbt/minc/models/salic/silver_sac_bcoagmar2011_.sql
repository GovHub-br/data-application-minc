-- Silver SALIC — silver_sac_bcoagmar2011_ (ex-sac__bcoagmar2011$).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_bcoagmar2011$, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 12 colunas: 1 tipadas, 10 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("bco") }} as bco,
    {{ bronze_inteiro("agencia") }} as agencia,
    {{ bronze_texto('"descriçao"') }} as descricao,
    {{ bronze_texto("uf") }} as uf,
    {{ bronze_texto('"endereço"') }} as endereco,
    {{ bronze_texto("bairro") }} as bairro,
    {{ bronze_texto("cidade") }} as cidade,
    {{ bronze_texto("cep") }} as cep,
    {{ bronze_texto("fone") }} as fone,
    {{ bronze_texto("fax") }} as fax,
    {{ bronze_texto("perfil") }} as perfil,
    _fatia
from {{ source("salic", "bronze_sac_bcoagmar2011$") }}
