-- Silver SALIC — silver_sac_tbpareceristaempenho (ex-sac__tbpareceristaempenho).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_tbpareceristaempenho, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 9 colunas: 7 tipadas, 1 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idpareceristaempenho") }} as idpareceristaempenho,
    {{ bronze_inteiro("idinexigibilidade") }} as idinexigibilidade,
    {{ bronze_texto("nrempenho") }} as nrempenho,
    {{ bronze_timestamp("dtempenho") }} as dtempenho,
    {{ bronze_inteiro("idagente") }} as idagente,
    {{ bronze_numerico("vlempenho") }} as vlempenho,
    {{ bronze_booleano("stestado") }} as stestado,
    {{ bronze_inteiro("idusuario") }} as idusuario,
    _fatia
from {{ source("salic", "bronze_sac_tbpareceristaempenho") }}
