-- Silver SALIC — silver_sac_empenhoprojeto (ex-sac__empenhoprojeto).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_empenhoprojeto, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 4 colunas: 3 tipadas, 0 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("codigoempenho") }} as codigoempenho,
    {{ bronze_inteiro("codigoconvenio") }} as codigoconvenio,
    {{ bronze_inteiro("logon") }} as logon,
    _fatia
from {{ source("salic", "bronze_sac_empenhoprojeto") }}
