-- Bronze SALIC — sac__documentosprojeto.
-- Origem: salic_bronze.sac__documentosprojeto, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 7 colunas: 4 tipadas, 2 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("contador") }} as contador,
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_texto("sequencial") }} as sequencial,
    {{ bronze_inteiro("codigodocumento") }} as codigodocumento,
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_inteiro("idprojeto") }} as idprojeto,
    _fatia
from {{ source("bronze_sac", "sac__documentosprojeto") }}
