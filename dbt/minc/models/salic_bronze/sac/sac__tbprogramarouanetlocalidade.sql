-- Bronze SALIC — sac__tbprogramarouanetlocalidade.
-- Origem: salic_bronze.sac__tbprogramarouanetlocalidade, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 5 colunas: 3 tipadas, 1 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idprogramarouanetlocalidade") }} as idprogramarouanetlocalidade,
    {{ bronze_inteiro("idprogramarouanet") }} as idprogramarouanet,
    {{ bronze_inteiro("iduf") }} as iduf,
    {{ bronze_texto("idmunicipio") }} as idmunicipio,
    _fatia
from {{ source("bronze_sac", "sac__tbprogramarouanetlocalidade") }}
