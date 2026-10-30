-- Silver SALIC — silver_sac_tblogintentativasacesso (ex-sac__tblogintentativasacesso).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_tblogintentativasacesso, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 6 colunas: 3 tipadas, 2 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("nrcpf") }} as nrcpf,
    {{ bronze_texto("nrip") }} as nrip,
    {{ bronze_timestamp("dttentativa") }} as dttentativa,
    {{ bronze_inteiro("nrtentativa") }} as nrtentativa,
    {{ bronze_inteiro("idlogintentativasacesso") }} as idlogintentativasacesso,
    _fatia
from {{ source("salic", "bronze_sac_tblogintentativasacesso") }}
