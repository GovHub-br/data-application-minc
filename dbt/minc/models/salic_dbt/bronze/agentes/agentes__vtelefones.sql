-- Bronze SALIC — agentes__vtelefones.
-- Origem: salic_bronze.agentes__vtelefones, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 9 colunas: 7 tipadas, 1 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idtelefone") }} as idtelefone,
    {{ bronze_inteiro("idagente") }} as idagente,
    {{ bronze_inteiro("tipotelefone") }} as tipotelefone,
    {{ bronze_inteiro("ddd") }} as ddd,
    {{ bronze_inteiro("numero") }} as numero,
    {{ bronze_inteiro("uf") }} as uf,
    {{ bronze_texto("divulgar") }} as divulgar,
    {{ bronze_inteiro("usuario") }} as usuario,
    _fatia
from {{ source("bronze_agentes", "agentes__vtelefones") }}
