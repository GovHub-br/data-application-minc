-- Bronze SALIC — sac__tbprogramarouanetdistribuirproposta.
-- Origem: salic_bronze.sac__tbprogramarouanetdistribuirproposta, onde tudo chega como
-- texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 7 colunas: 4 tipadas, 2 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idprogramarouanetdistribuirproposta") }}
    as idprogramarouanetdistribuirproposta,
    {{ bronze_inteiro("idprogramarouanet") }} as idprogramarouanet,
    {{ bronze_inteiro("idpreprojeto") }} as idpreprojeto,
    {{ bronze_inteiro("idavaliador") }} as idavaliador,
    {{ bronze_texto("tpanalise") }} as tpanalise,
    {{ bronze_texto("tpavaliacao") }} as tpavaliacao,
    _fatia
from {{ source("bronze_sac", "sac__tbprogramarouanetdistribuirproposta") }}
