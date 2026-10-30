-- Silver SALIC — silver_sac_tbpareceristaprojetoredistribuido
-- (ex-sac__tbpareceristaprojetoredistribuido).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_tbpareceristaprojetoredistribuido, onde tudo chega como
-- texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 7 colunas: 5 tipadas, 1 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idpareceristaprojetoredistribuido") }}
    as idpareceristaprojetoredistribuido,
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_inteiro("idparecerista") }} as idparecerista,
    {{ bronze_timestamp("dtredistribuicao") }} as dtredistribuicao,
    {{ bronze_texto("tpanalise") }} as tpanalise,
    {{ bronze_booleano("simotivo") }} as simotivo,
    _fatia
from {{ source("salic", "bronze_sac_tbpareceristaprojetoredistribuido") }}
