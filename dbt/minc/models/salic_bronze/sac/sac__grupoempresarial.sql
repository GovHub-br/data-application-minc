-- Bronze SALIC — sac__grupoempresarial.
-- Origem: salic_bronze.sac__grupoempresarial, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 10 colunas: 2 tipadas, 7 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("codigo") }} as codigo,
    {{ bronze_texto("nomegrupo") }} as nomegrupo,
    {{ bronze_booleano("publica") }} as publica,
    {{ bronze_texto("endereco") }} as endereco,
    {{ bronze_texto("cidade") }} as cidade,
    {{ bronze_texto("uf") }} as uf,
    {{ bronze_texto("cep") }} as cep,
    {{ bronze_texto("pais") }} as pais,
    {{ bronze_texto("presidente") }} as presidente,
    _fatia
from {{ source("bronze_sac", "sac__grupoempresarial") }}
