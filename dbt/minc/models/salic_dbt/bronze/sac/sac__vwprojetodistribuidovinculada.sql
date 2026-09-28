-- Bronze SALIC — sac__vwprojetodistribuidovinculada.
-- Origem: salic_bronze.sac__vwprojetodistribuidovinculada, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 13 colunas: 6 tipadas, 6 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_texto("pronac") }} as pronac,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_inteiro("idproduto") }} as idproduto,
    {{ bronze_texto("produto") }} as produto,
    {{ bronze_texto("descricaoanalise") }} as descricaoanalise,
    {{ bronze_inteiro("tipoanalise") }} as tipoanalise,
    {{ bronze_texto("orgao") }} as orgao,
    {{ bronze_inteiro("idorgao") }} as idorgao,
    {{ bronze_timestamp("dtenvio") }} as dtenvio,
    {{ bronze_inteiro("nrdias") }} as nrdias,
    {{ bronze_texto("situacao") }} as situacao,
    _fatia
from {{ source("bronze_sac", "sac__vwprojetodistribuidovinculada") }}
