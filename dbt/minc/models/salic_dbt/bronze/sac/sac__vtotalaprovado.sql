-- Bronze SALIC — sac__vtotalaprovado.
-- Origem: salic_bronze.sac__vtotalaprovado, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 11 colunas: 9 tipadas, 1 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_inteiro("sequencial") }} as sequencial,
    {{ bronze_timestamp("dtaprovacao") }} as dtaprovacao,
    {{ bronze_timestamp("dtiniciocaptacao") }} as dtiniciocaptacao,
    {{ bronze_timestamp("dtfimcaptacao") }} as dtfimcaptacao,
    {{ bronze_numerico("aprovadoufir") }} as aprovadoufir,
    {{ bronze_numerico("aprovadoreal") }} as aprovadoreal,
    {{ bronze_numerico("concedidocusteioreal") }} as concedidocusteioreal,
    {{ bronze_numerico("concedidocapitalreal") }} as concedidocapitalreal,
    {{ bronze_numerico("contrapartidareal") }} as contrapartidareal,
    _fatia
from {{ source("bronze_sac", "sac__vtotalaprovado") }}
