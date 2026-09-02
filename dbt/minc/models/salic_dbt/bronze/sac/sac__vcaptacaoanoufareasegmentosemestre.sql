-- Bronze SALIC — sac__vcaptacaoanoufareasegmentosemestre.
-- Origem: salic_bronze.sac__vcaptacaoanoufareasegmentosemestre, onde tudo chega como
-- texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 8 colunas: 5 tipadas, 2 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("ano") }} as ano,
    {{ bronze_texto("ufprojeto") }} as ufprojeto,
    {{ bronze_inteiro("area") }} as area,
    {{ bronze_texto("segmento") }} as segmento,
    {{ bronze_numerico("semestre_1") }} as semestre_1,
    {{ bronze_numerico("semestre_2") }} as semestre_2,
    {{ bronze_numerico("total") }} as total,
    _fatia
from {{ source("bronze_sac", "sac__vcaptacaoanoufareasegmentosemestre") }}
