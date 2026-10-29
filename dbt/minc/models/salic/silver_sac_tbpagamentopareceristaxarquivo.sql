-- Silver SALIC — silver_sac_tbpagamentopareceristaxarquivo (ex-sac__tbpagamentopareceristaxarquivo).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_tbpagamentopareceristaxarquivo, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 4 colunas: 2 tipadas, 1 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idgerarpagamentoparecerista") }} as idgerarpagamentoparecerista,
    {{ bronze_inteiro("idarquivo") }} as idarquivo,
    {{ bronze_texto("siarquivo") }} as siarquivo,
    _fatia
from {{ source("salic", "bronze_sac_tbpagamentopareceristaxarquivo") }}
