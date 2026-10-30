-- Silver SALIC — silver_sac_vwalteraranalistaproposta (ex-sac__vwalteraranalistaproposta).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_vwalteraranalistaproposta, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 6 colunas: 2 tipadas, 3 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idproposta") }} as idproposta,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_texto("orgao") }} as orgao,
    {{ bronze_inteiro("idtecnico") }} as idtecnico,
    {{ bronze_texto("tecnico") }} as tecnico,
    _fatia
from {{ source("salic", "bronze_sac_vwalteraranalistaproposta") }}
