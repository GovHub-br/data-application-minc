-- Bronze SALIC — sac__tbcontabancariabloqueada.
-- Origem: salic_bronze.sac__tbcontabancariabloqueada, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 8 colunas: 6 tipadas, 1 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idcontabancariabloqueada") }} as idcontabancariabloqueada,
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_timestamp("dtliberacao") }} as dtliberacao,
    {{ bronze_timestamp("dtbloqueio") }} as dtbloqueio,
    {{ bronze_texto("tpbloqueio") }} as tpbloqueio,
    {{ bronze_inteiro("idusuarioliberouconta") }} as idusuarioliberouconta,
    {{ bronze_numerico("vlliberado") }} as vlliberado,
    _fatia
from {{ source("bronze_sac", "sac__tbcontabancariabloqueada") }}
