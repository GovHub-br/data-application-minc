-- Silver SALIC — silver_sac_documentosexigidos (ex-sac__documentosexigidos).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_documentosexigidos, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 7 colunas: 4 tipadas, 2 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("codigo") }} as codigo,
    {{ bronze_texto("descricao") }} as descricao,
    {{ bronze_texto("area") }} as area,
    {{ bronze_inteiro("opcao") }} as opcao,
    {{ bronze_booleano("stestado") }} as stestado,
    {{ bronze_booleano("stupload") }} as stupload,
    _fatia
from {{ source("salic", "bronze_sac_documentosexigidos") }}
