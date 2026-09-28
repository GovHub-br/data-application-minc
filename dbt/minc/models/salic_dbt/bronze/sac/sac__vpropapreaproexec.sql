-- Bronze SALIC — sac__vpropapreaproexec.
-- Origem: salic_bronze.sac__vpropapreaproexec, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 12 colunas: 6 tipadas, 5 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_inteiro("sequencial") }} as sequencial,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_numerico("aprovado") }} as aprovado,
    {{ bronze_numerico("captado") }} as captado,
    {{ bronze_texto("executado") }} as executado,
    {{ bronze_texto("cgccpf") }} as cgccpf,
    {{ bronze_inteiro("area") }} as area,
    {{ bronze_inteiro("segmento") }} as segmento,
    {{ bronze_inteiro("mecanismo") }} as mecanismo,
    {{ bronze_texto("situacao") }} as situacao,
    _fatia
from {{ source("bronze_sac", "sac__vpropapreaproexec") }}
