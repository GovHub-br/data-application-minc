-- Silver SALIC — silver_sac_vfunarte (ex-sac__vfunarte).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_vfunarte, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 8 colunas: 2 tipadas, 5 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_texto("sequencial") }} as sequencial,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_texto("tipoparecer") }} as tipoparecer,
    {{ bronze_timestamp("dtparecer") }} as dtparecer,
    {{ bronze_texto("parecerista") }} as parecerista,
    {{ bronze_numerico("aprovado") }} as aprovado,
    _fatia
from {{ source("salic", "bronze_sac_vfunarte") }}
