-- Bronze SALIC — sac__dba_captacaoanoufmunicipio.
-- Origem: salic_bronze.sac__dba_captacaoanoufmunicipio, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 7 colunas: 2 tipadas, 4 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("ano_captacao") }} as ano_captacao,
    {{ bronze_texto("regiao") }} as regiao,
    {{ bronze_texto("uf") }} as uf,
    {{ bronze_texto("municipio") }} as municipio,
    {{ bronze_texto("pronac") }} as pronac,
    {{ bronze_numerico("valor_captado") }} as valor_captado,
    _fatia
from {{ source("bronze_sac", "sac__dba_captacaoanoufmunicipio") }}
