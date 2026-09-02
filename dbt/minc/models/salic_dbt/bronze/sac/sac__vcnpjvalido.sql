-- Bronze SALIC — sac__vcnpjvalido.
-- Origem: salic_bronze.sac__vcnpjvalido, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 7 colunas: 2 tipadas, 4 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_inteiro("sequencial") }} as sequencial,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_texto("cgccpf") }} as cgccpf,
    {{ bronze_inteiro("cnpjvalido") }} as cnpjvalido,
    {{ bronze_texto("nome") }} as nome,
    _fatia
from {{ source("bronze_sac", "sac__vcnpjvalido") }}
