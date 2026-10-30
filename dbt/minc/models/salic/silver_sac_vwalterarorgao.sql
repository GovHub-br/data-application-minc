-- Silver SALIC — silver_sac_vwalterarorgao (ex-sac__vwalterarorgao).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_vwalterarorgao, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 5 colunas: 2 tipadas, 2 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("pronac") }} as pronac,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_inteiro("orgao") }} as orgao,
    {{ bronze_inteiro("logon") }} as logon,
    _fatia
from {{ source("salic", "bronze_sac_vwalterarorgao") }}
