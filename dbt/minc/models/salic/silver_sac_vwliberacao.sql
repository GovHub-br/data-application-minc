-- Silver SALIC — silver_sac_vwliberacao (ex-sac__vwliberacao).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_vwliberacao, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 11 colunas: 1 tipadas, 9 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_texto("sequencial") }} as sequencial,
    {{ bronze_texto("pronac") }} as pronac,
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_texto("situacao") }} as situacao,
    {{ bronze_texto("cgccpf") }} as cgccpf,
    {{ bronze_texto("inabilitado") }} as inabilitado,
    {{ bronze_texto("certidao") }} as certidao,
    {{ bronze_texto("cadin") }} as cadin,
    _fatia
from {{ source("salic", "bronze_sac_vwliberacao") }}
