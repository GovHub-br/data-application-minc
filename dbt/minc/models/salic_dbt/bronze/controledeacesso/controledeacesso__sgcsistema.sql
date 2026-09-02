-- Bronze SALIC — controledeacesso__sgcsistema.
-- Origem: salic_bronze.controledeacesso__sgcsistema, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 4 colunas: 1 tipadas, 2 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idsistema") }} as idsistema,
    {{ bronze_texto("nomesistema") }} as nomesistema,
    {{ bronze_texto("descricaosistema") }} as descricaosistema,
    _fatia
from {{ source("bronze_controledeacesso", "controledeacesso__sgcsistema") }}
