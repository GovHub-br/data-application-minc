-- Bronze SALIC — sac__tbitensplanilhaproduto.
-- Origem: salic_bronze.sac__tbitensplanilhaproduto, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 6 colunas: 5 tipadas, 0 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("iditensplanilhaproduto") }} as iditensplanilhaproduto,
    {{ bronze_inteiro("idproduto") }} as idproduto,
    {{ bronze_inteiro("idplanilhaetapa") }} as idplanilhaetapa,
    {{ bronze_inteiro("idplanilhaitens") }} as idplanilhaitens,
    {{ bronze_inteiro("idusuario") }} as idusuario,
    _fatia
from {{ source("bronze_sac", "sac__tbitensplanilhaproduto") }}
