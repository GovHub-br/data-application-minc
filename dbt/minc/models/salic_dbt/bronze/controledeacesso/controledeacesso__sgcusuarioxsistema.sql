-- Bronze SALIC — controledeacesso__sgcusuarioxsistema.
-- Origem: salic_bronze.controledeacesso__sgcusuarioxsistema, onde tudo chega como texto
-- da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 3 colunas: 2 tipadas, 0 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idusuario") }} as idusuario,
    {{ bronze_inteiro("idsistema") }} as idsistema,
    _fatia
from {{ source("bronze_controledeacesso", "controledeacesso__sgcusuarioxsistema") }}
