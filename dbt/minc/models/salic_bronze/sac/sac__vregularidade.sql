-- Bronze SALIC — sac__vregularidade.
-- Origem: salic_bronze.sac__vregularidade, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 11 colunas: 2 tipadas, 8 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_inteiro("sequencial") }} as sequencial,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_texto("orgao") }} as orgao,
    {{ bronze_texto("observacao") }} as observacao,
    {{ bronze_texto("pronac") }} as pronac,
    {{ bronze_texto("situacao") }} as situacao,
    {{ bronze_timestamp("dtsituacao") }} as dtsituacao,
    {{ bronze_texto("gerador") }} as gerador,
    {{ bronze_texto("cgccpf") }} as cgccpf,
    _fatia
from {{ source("bronze_sac", "sac__vregularidade") }}
