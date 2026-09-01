-- Bronze SALIC — sac__vcaptacaopormptipoapoiomecena.
-- Origem: salic_bronze.sac__vcaptacaopormptipoapoiomecena, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 8 colunas: 5 tipadas, 2 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("ano") }} as ano,
    {{ bronze_inteiro("mes") }} as mes,
    {{ bronze_texto("mp") }} as mp,
    {{ bronze_texto("tipoapoio") }} as tipoapoio,
    {{ bronze_inteiro("cgccpf", tipo="bigint") }} as cgccpf,
    {{ bronze_numerico("apoioufir") }} as apoioufir,
    {{ bronze_numerico("apoioreal") }} as apoioreal,
    _fatia
from {{ source("bronze_sac", "sac__vcaptacaopormptipoapoiomecena") }}
