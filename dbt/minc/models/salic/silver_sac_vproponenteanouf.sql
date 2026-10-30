-- Silver SALIC — silver_sac_vproponenteanouf (ex-sac__vproponenteanouf).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_vproponenteanouf, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 5 colunas: 1 tipadas, 3 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("ano") }} as ano,
    {{ bronze_texto("cgccpf") }} as cgccpf,
    {{ bronze_texto("nome") }} as nome,
    {{ bronze_texto("uf") }} as uf,
    _fatia
from {{ source("salic", "bronze_sac_vproponenteanouf") }}
