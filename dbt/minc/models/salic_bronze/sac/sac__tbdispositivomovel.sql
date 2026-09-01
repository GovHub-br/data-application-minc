-- Bronze SALIC — sac__tbdispositivomovel.
-- Origem: salic_bronze.sac__tbdispositivomovel, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 6 colunas: 3 tipadas, 2 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("idregistration") }} as idregistration,
    {{ bronze_timestamp("dtregistration") }} as dtregistration,
    {{ bronze_texto("nrcpf") }} as nrcpf,
    {{ bronze_timestamp("dtacesso") }} as dtacesso,
    {{ bronze_inteiro("iddispositivomovel") }} as iddispositivomovel,
    _fatia
from {{ source("bronze_sac", "sac__tbdispositivomovel") }}
