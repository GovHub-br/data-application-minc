-- Bronze SALIC — sac__tbprodutoxtipicidade.
-- Origem: salic_bronze.sac__tbprodutoxtipicidade, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 8 colunas: 5 tipadas, 2 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idproduto") }} as idproduto,
    {{ bronze_inteiro("idtipicidade") }} as idtipicidade,
    {{ bronze_inteiro("idtipologia") }} as idtipologia,
    {{ bronze_texto("cdarea") }} as cdarea,
    {{ bronze_texto("cdsegmento") }} as cdsegmento,
    {{ bronze_inteiro("idprodutoxtipicidade") }} as idprodutoxtipicidade,
    {{ bronze_booleano("stprincipal") }} as stprincipal,
    _fatia
from {{ source("bronze_sac", "sac__tbprodutoxtipicidade") }}
