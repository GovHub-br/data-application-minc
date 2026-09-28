-- Bronze SALIC — sac__vwprecomedio_produto_item_unidade_uf_municipio.
-- Origem: salic_bronze.sac__vwprecomedio_produto_item_unidade_uf_municipio, onde tudo
-- chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 14 colunas: 7 tipadas, 6 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("idproduto") }} as idproduto,
    {{ bronze_texto("produto") }} as produto,
    {{ bronze_inteiro("idplanilhaitem") }} as idplanilhaitem,
    {{ bronze_texto("item") }} as item,
    {{ bronze_inteiro("idunidade") }} as idunidade,
    {{ bronze_texto("unidade") }} as unidade,
    {{ bronze_inteiro("idufdespesa") }} as idufdespesa,
    {{ bronze_texto("uf") }} as uf,
    {{ bronze_inteiro("idmunicipiodespesa") }} as idmunicipiodespesa,
    {{ bronze_texto("cidade") }} as cidade,
    {{ bronze_numerico("preco_minimo") }} as preco_minimo,
    {{ bronze_numerico("preco_medio") }} as preco_medio,
    {{ bronze_numerico("preco_maximo") }} as preco_maximo,
    _fatia
from {{ source("bronze_sac", "sac__vwprecomedio_produto_item_unidade_uf_municipio") }}
