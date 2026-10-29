-- Silver SALIC — silver_tabelas_mei_gruposxservicos_intranet (ex-tabelas__mei_gruposxservicos_intranet).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_tabelas_mei_gruposxservicos_intranet, onde tudo chega como
-- texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 3 colunas: 2 tipadas, 0 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("gxs_grupo") }} as gxs_grupo,
    {{ bronze_inteiro("gxs_servico") }} as gxs_servico,
    _fatia
from {{ source("salic", "bronze_tabelas_mei_gruposxservicos_intranet") }}
