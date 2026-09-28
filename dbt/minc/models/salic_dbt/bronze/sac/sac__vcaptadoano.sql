-- Bronze SALIC — sac__vcaptadoano.
-- Origem: salic_bronze.sac__vcaptadoano, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 7 colunas: 3 tipadas, 3 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("anorecibo") }} as anorecibo,
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_texto("sequencial") }} as sequencial,
    {{ bronze_texto("medidaprovisoria") }} as medidaprovisoria,
    {{ bronze_numerico("captacaoufir") }} as captacaoufir,
    {{ bronze_numerico("captacaoreal") }} as captacaoreal,
    _fatia
from {{ source("bronze_sac", "sac__vcaptadoano") }}
