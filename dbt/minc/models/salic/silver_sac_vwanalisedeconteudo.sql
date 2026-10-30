-- Silver SALIC — silver_sac_vwanalisedeconteudo (ex-sac__vwanalisedeconteudo).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_vwanalisedeconteudo, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 6 colunas: 3 tipadas, 2 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idanalisedeconteudo") }} as idanalisedeconteudo,
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_inteiro("idproduto") }} as idproduto,
    {{ bronze_texto("produto") }} as produto,
    _fatia
from {{ source("salic", "bronze_sac_vwanalisedeconteudo") }}
