-- Silver SALIC — silver_sac_dba_total_investido_investidor_ano
-- (ex-sac__dba_total_investido_investidor_ano).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_dba_total_investido_investidor_ano, onde tudo chega como
-- texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 4 colunas: 2 tipadas, 1 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("cnpj_cpf") }} as cnpj_cpf,
    {{ bronze_inteiro("ano_investimento") }} as ano_investimento,
    {{ bronze_numerico("valor_investido_total") }} as valor_investido_total,
    _fatia
from {{ source("salic", "bronze_sac_dba_total_investido_investidor_ano") }}
