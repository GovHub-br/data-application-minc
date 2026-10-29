-- Silver SALIC — silver_sac_vproponentecaptacaoanouf (ex-sac__vproponentecaptacaoanouf).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_vproponentecaptacaoanouf, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 6 colunas: 2 tipadas, 3 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("ano") }} as ano,
    {{ bronze_texto("cgccpf") }} as cgccpf,
    {{ bronze_texto("nome") }} as nome,
    {{ bronze_texto("uf") }} as uf,
    {{ bronze_numerico("captacao") }} as captacao,
    _fatia
from {{ source("salic", "bronze_sac_vproponentecaptacaoanouf") }}
