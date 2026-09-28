-- Bronze SALIC — sac__historicoaspectosfisicosx.
-- Origem: salic_bronze.sac__historicoaspectosfisicosx, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 8 colunas: 4 tipadas, 3 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("contador") }} as contador,
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_texto("sequencial") }} as sequencial,
    {{ bronze_texto("itensfisicos") }} as itensfisicos,
    {{ bronze_inteiro("proposta") }} as proposta,
    {{ bronze_inteiro("prcont") }} as prcont,
    {{ bronze_inteiro("logon") }} as logon,
    _fatia
from {{ source("bronze_sac", "sac__historicoaspectosfisicosx") }}
