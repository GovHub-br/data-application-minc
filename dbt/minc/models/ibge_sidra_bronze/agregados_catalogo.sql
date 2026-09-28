-- Bronze ibge_sidra — agregados_catalogo.
-- Origem: ibge_sidra.agregados_catalogo, onde tudo chega como text da ingestão via API.
-- Tipar é o trabalho desta camada.
-- 4 colunas: 1 tipadas, 3 mantidas como texto.
-- O cast vem do padrão medido no dado (scripts/perfilar_padroes.py),
-- não do nome da coluna: exige 100% dos valores preenchidos casando.
select
    {{ bronze_inteiro("id") }} as id,
    {{ bronze_texto("nome") }} as nome,
    {{ bronze_texto("pesquisa_id") }} as pesquisa_id,
    {{ bronze_texto("pesquisa_nome") }} as pesquisa_nome
from {{ source("ibge_sidra", "agregados_catalogo") }}
