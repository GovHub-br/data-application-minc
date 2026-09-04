-- Bronze SALIC — agentes__populacaomunicipio.
-- Origem: salic_bronze.agentes__populacaomunicipio, onde tudo chega como texto
-- da ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 4 colunas: 2 tipadas, 2 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
--
-- Os dois códigos ficam como texto: `idmunicipio` tem 6 posições e
-- `idmunicipio7` tem 7, e é justamente carregar as duas formas que faz desta
-- tabela a ponte entre o código do SALIC e o do IBGE usado pelo transferegov.
select
    {{ bronze_texto("idmunicipio") }} as idmunicipio,
    {{ bronze_texto("idmunicipio7") }} as idmunicipio7,
    {{ bronze_inteiro("populacao") }} as populacao,
    _fatia
from {{ source("bronze_agentes", "agentes__populacaomunicipio") }}
