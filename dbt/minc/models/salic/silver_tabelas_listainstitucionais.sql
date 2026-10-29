-- Silver SALIC — silver_tabelas_listainstitucionais (ex-tabelas__listainstitucionais).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_tabelas_listainstitucionais, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 2 colunas: 0 tipadas, 1 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select {{ bronze_texto("nome") }} as nome, _fatia
from {{ source("salic", "bronze_tabelas_listainstitucionais") }}
