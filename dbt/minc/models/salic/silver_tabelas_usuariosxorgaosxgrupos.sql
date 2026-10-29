-- Silver SALIC — silver_tabelas_usuariosxorgaosxgrupos (ex-tabelas__usuariosxorgaosxgrupos).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_tabelas_usuariosxorgaosxgrupos, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 5 colunas: 4 tipadas, 0 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("uog_usuario") }} as uog_usuario,
    {{ bronze_inteiro("uog_orgao") }} as uog_orgao,
    {{ bronze_inteiro("uog_grupo") }} as uog_grupo,
    {{ bronze_inteiro("uog_status") }} as uog_status,
    _fatia
from {{ source("salic", "bronze_tabelas_usuariosxorgaosxgrupos") }}
