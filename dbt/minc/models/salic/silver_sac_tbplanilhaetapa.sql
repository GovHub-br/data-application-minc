-- Silver SALIC — silver_sac_tbplanilhaetapa (ex-sac__tbplanilhaetapa).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_tbplanilhaetapa, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 7 colunas: 3 tipadas, 3 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idplanilhaetapa") }} as idplanilhaetapa,
    {{ bronze_texto("descricao") }} as descricao,
    {{ bronze_texto("tpcusto") }} as tpcusto,
    {{ bronze_booleano("stestado") }} as stestado,
    {{ bronze_texto("tpgrupo") }} as tpgrupo,
    {{ bronze_inteiro("nrordenacao") }} as nrordenacao,
    _fatia
from {{ source("salic", "bronze_sac_tbplanilhaetapa") }}
