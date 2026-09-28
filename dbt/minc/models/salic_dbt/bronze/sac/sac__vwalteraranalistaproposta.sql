-- Bronze SALIC — sac__vwalteraranalistaproposta.
-- Origem: salic_bronze.sac__vwalteraranalistaproposta, onde tudo chega como texto da
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
from {{ source("bronze_sac", "sac__vwalteraranalistaproposta") }}
