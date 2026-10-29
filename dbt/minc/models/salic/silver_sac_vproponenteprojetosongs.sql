-- Silver SALIC — silver_sac_vproponenteprojetosongs (ex-sac__vproponenteprojetosongs).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_vproponenteprojetosongs, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 6 colunas: 1 tipadas, 4 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("cgccpf") }} as cgccpf,
    {{ bronze_texto("nome") }} as nome,
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_texto("sequencial") }} as sequencial,
    {{ bronze_numerico("captacaoreal") }} as captacaoreal,
    _fatia
from {{ source("salic", "bronze_sac_vproponenteprojetosongs") }}
