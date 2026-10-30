-- Silver SALIC — silver_tabelas_ect_localidades (ex-tabelas__ect_localidades).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_tabelas_ect_localidades, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 6 colunas: 0 tipadas, 5 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("ectl_uf") }} as ectl_uf,
    {{ bronze_texto("ectl_hash") }} as ectl_hash,
    {{ bronze_texto("ectl_local") }} as ectl_local,
    {{ bronze_texto("ectl_tipo_local") }} as ectl_tipo_local,
    {{ bronze_texto("ectl_estruturado") }} as ectl_estruturado,
    _fatia
from {{ source("salic", "bronze_tabelas_ect_localidades") }}
