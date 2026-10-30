-- Silver SALIC — silver_sac_dba_valor_solicitado_captado_projeto
-- (ex-sac__dba_valor_solicitado_captado_projeto).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_dba_valor_solicitado_captado_projeto, onde tudo chega como
-- texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 9 colunas: 4 tipadas, 4 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("projeto_enquadramento") }} as projeto_enquadramento,
    {{ bronze_texto("projeto_ano") }} as projeto_ano,
    {{ bronze_texto("projeto_sequencial") }} as projeto_sequencial,
    {{ bronze_inteiro("projeto_area") }} as projeto_area,
    {{ bronze_texto("projeto_situacao") }} as projeto_situacao,
    {{ bronze_numerico("valor_solicitado") }} as valor_solicitado,
    {{ bronze_numerico("valor_captado") }} as valor_captado,
    {{ bronze_inteiro("ano_recibo") }} as ano_recibo,
    _fatia
from {{ source("salic", "bronze_sac_dba_valor_solicitado_captado_projeto") }}
