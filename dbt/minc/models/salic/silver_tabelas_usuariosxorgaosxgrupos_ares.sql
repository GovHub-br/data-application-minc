-- Silver SALIC — silver_tabelas_usuariosxorgaosxgrupos_ares
-- (ex-tabelas__usuariosxorgaosxgrupos_ares).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_tabelas_usuariosxorgaosxgrupos_ares, onde tudo chega como texto
-- da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 4 colunas: 3 tipadas, 0 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("uog_usuario") }} as uog_usuario,
    {{ bronze_inteiro("uog_orgao") }} as uog_orgao,
    {{ bronze_inteiro("uog_grupo") }} as uog_grupo,
    _fatia
from {{ source("salic", "bronze_tabelas_usuariosxorgaosxgrupos_ares") }}
