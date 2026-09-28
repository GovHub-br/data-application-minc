-- Bronze SALIC — sac__vproponenteprojetos.
-- Origem: salic_bronze.sac__vproponenteprojetos, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 7 colunas: 3 tipadas, 3 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("cgccpf") }} as cgccpf,
    {{ bronze_texto("nome") }} as nome,
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_inteiro("sequencial") }} as sequencial,
    {{ bronze_numerico("captacaoufir") }} as captacaoufir,
    {{ bronze_numerico("captacaoreal") }} as captacaoreal,
    _fatia
from {{ source("bronze_sac", "sac__vproponenteprojetos") }}
