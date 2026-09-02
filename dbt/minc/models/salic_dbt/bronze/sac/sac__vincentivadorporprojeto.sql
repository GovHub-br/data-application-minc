-- Bronze SALIC — sac__vincentivadorporprojeto.
-- Origem: salic_bronze.sac__vincentivadorporprojeto, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 6 colunas: 3 tipadas, 2 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("cgccpf") }} as cgccpf,
    {{ bronze_inteiro("anoprojeto") }} as anoprojeto,
    {{ bronze_texto("sequencial") }} as sequencial,
    {{ bronze_numerico("apoioreal") }} as apoioreal,
    {{ bronze_data("dtcaptacao") }} as dtcaptacao,
    _fatia
from {{ source("bronze_sac", "sac__vincentivadorporprojeto") }}
