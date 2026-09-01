-- Bronze SALIC — sac__curriculo.
-- Origem: salic_bronze.sac__curriculo, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 5 colunas: 3 tipadas, 1 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("cgccpf") }} as cgccpf,
    {{ bronze_inteiro("nivel") }} as nivel,
    {{ bronze_booleano("cadastrosav") }} as cadastrosav,
    {{ bronze_inteiro("logon") }} as logon,
    _fatia
from {{ source("bronze_sac", "sac__curriculo") }}
