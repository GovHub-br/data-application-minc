-- Bronze SALIC — sac__tbpareceristainexigibilidade.
-- Origem: salic_bronze.sac__tbpareceristainexigibilidade, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 8 colunas: 6 tipadas, 1 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idpareceristainexigibilidade") }} as idpareceristainexigibilidade,
    {{ bronze_timestamp("dtinexigibilidade") }} as dtinexigibilidade,
    {{ bronze_inteiro("idagente") }} as idagente,
    {{ bronze_numerico("vlinexigibilidade") }} as vlinexigibilidade,
    {{ bronze_booleano("stestado") }} as stestado,
    {{ bronze_inteiro("idusuario") }} as idusuario,
    {{ bronze_texto("identificacaoinex") }} as identificacaoinex,
    _fatia
from {{ source("bronze_sac", "sac__tbpareceristainexigibilidade") }}
