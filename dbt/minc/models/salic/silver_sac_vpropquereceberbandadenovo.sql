-- Silver SALIC — silver_sac_vpropquereceberbandadenovo
-- (ex-sac__vpropquereceberbandadenovo).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_vpropquereceberbandadenovo, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 8 colunas: 0 tipadas, 7 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("cgccpf") }} as cgccpf,
    {{ bronze_texto("proponente") }} as proponente,
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_texto("sequencial") }} as sequencial,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_texto("situacao") }} as situacao,
    {{ bronze_texto("descsituacao") }} as descsituacao,
    _fatia
from {{ source("salic", "bronze_sac_vpropquereceberbandadenovo") }}
