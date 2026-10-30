-- Silver SALIC — silver_sac_tbreadequacaoxtbdiligencia (ex-sac__tbreadequacaoxtbdiligencia).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_tbreadequacaoxtbdiligencia, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 3 colunas: 2 tipadas, 0 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idreadequacao") }} as idreadequacao,
    {{ bronze_inteiro("iddiligencia") }} as iddiligencia,
    _fatia
from {{ source("salic", "bronze_sac_tbreadequacaoxtbdiligencia") }}
