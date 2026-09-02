-- Bronze SALIC — sac__vproponentetipopessoa.
-- Origem: salic_bronze.sac__vproponentetipopessoa, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 9 colunas: 5 tipadas, 3 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("cgccpf") }} as cgccpf,
    {{ bronze_texto("tipopessoa") }} as tipopessoa,
    {{ bronze_texto("proponente") }} as proponente,
    {{ bronze_inteiro("mecanismo") }} as mecanismo,
    {{ bronze_numerico("solicitado") }} as solicitado,
    {{ bronze_numerico("aprovado") }} as aprovado,
    {{ bronze_numerico("captado") }} as captado,
    {{ bronze_numerico("saldo") }} as saldo,
    _fatia
from {{ source("bronze_sac", "sac__vproponentetipopessoa") }}
