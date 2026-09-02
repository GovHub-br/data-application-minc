-- Bronze SALIC — tabelas__ect_bairros.
-- Origem: salic_bronze.tabelas__ect_bairros, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 5 colunas: 1 tipadas, 3 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("ectb_estruturado") }} as ectb_estruturado,
    {{ bronze_texto("ectb_hash") }} as ectb_hash,
    {{ bronze_texto("ectb_abreviado") }} as ectb_abreviado,
    {{ bronze_inteiro("ectb_cod_bairro") }} as ectb_cod_bairro,
    _fatia
from {{ source("bronze_tabelas", "tabelas__ect_bairros") }}
