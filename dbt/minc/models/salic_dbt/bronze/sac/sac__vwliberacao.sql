-- Bronze SALIC — sac__vwliberacao.
-- Origem: salic_bronze.sac__vwliberacao, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 11 colunas: 3 tipadas, 7 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("anoprojeto") }} as anoprojeto,
    {{ bronze_texto("sequencial") }} as sequencial,
    {{ bronze_inteiro("pronac") }} as pronac,
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_texto("situacao") }} as situacao,
    {{ bronze_texto("cgccpf") }} as cgccpf,
    {{ bronze_texto("inabilitado") }} as inabilitado,
    {{ bronze_texto("certidao") }} as certidao,
    {{ bronze_texto("cadin") }} as cadin,
    _fatia
from {{ source("bronze_sac", "sac__vwliberacao") }}
