-- Silver SALIC — silver_sac_vprojprazodecaptvencnoexerc
-- (ex-sac__vprojprazodecaptvencnoexerc).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_vprojprazodecaptvencnoexerc, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 8 colunas: 4 tipadas, 3 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("nrprojeto") }} as nrprojeto,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_texto("situacao") }} as situacao,
    {{ bronze_timestamp("dtfimcaptacao") }} as dtfimcaptacao,
    {{ bronze_numerico("aprovado") }} as aprovado,
    {{ bronze_numerico("captado") }} as captado,
    {{ bronze_numerico("saldo") }} as saldo,
    _fatia
from {{ source("salic", "bronze_sac_vprojprazodecaptvencnoexerc") }}
