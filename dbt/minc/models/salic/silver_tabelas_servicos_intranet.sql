-- Silver SALIC — silver_tabelas_servicos_intranet (ex-tabelas__servicos_intranet).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_tabelas_servicos_intranet, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 7 colunas: 1 tipadas, 5 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("srv_codigo") }} as srv_codigo,
    {{ bronze_texto("srv_titulo") }} as srv_titulo,
    {{ bronze_texto("srv_link") }} as srv_link,
    {{ bronze_texto("srv_janela") }} as srv_janela,
    {{ bronze_texto("srv_ordem") }} as srv_ordem,
    {{ bronze_texto("srv_seguranca") }} as srv_seguranca,
    _fatia
from {{ source("salic", "bronze_tabelas_servicos_intranet") }}
