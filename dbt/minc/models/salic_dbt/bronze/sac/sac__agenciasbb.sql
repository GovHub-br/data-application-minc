-- Bronze SALIC — sac__agenciasbb.
-- Origem: salic_bronze.sac__agenciasbb, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 8 colunas: 1 tipadas, 6 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("agencia") }} as agencia,
    {{ bronze_texto("nome") }} as nome,
    {{ bronze_texto("uf") }} as uf,
    {{ bronze_inteiro("perfil") }} as perfil,
    {{ bronze_texto("logradouro_completo") }} as logradouro_completo,
    {{ bronze_texto("bairro") }} as bairro,
    {{ bronze_texto("municipio") }} as municipio,
    _fatia
from {{ source("bronze_sac", "sac__agenciasbb") }}
