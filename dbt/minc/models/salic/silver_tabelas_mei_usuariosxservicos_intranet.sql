-- Silver SALIC — silver_tabelas_mei_usuariosxservicos_intranet (ex-tabelas__mei_usuariosxservicos_intranet).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_tabelas_mei_usuariosxservicos_intranet, onde tudo chega como
-- texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 3 colunas: 2 tipadas, 0 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("uxs_usuario") }} as uxs_usuario,
    {{ bronze_inteiro("uxs_servicos") }} as uxs_servicos,
    _fatia
from {{ source("salic", "bronze_tabelas_mei_usuariosxservicos_intranet") }}
