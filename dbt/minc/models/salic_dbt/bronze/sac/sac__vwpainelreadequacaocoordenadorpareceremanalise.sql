-- Bronze SALIC — sac__vwpainelreadequacaocoordenadorpareceremanalise.
-- Origem: salic_bronze.sac__vwpainelreadequacaocoordenadorpareceremanalise, onde tudo
-- chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 13 colunas: 8 tipadas, 4 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_inteiro("idreadequacao") }} as idreadequacao,
    {{ bronze_texto("dsavaliacao") }} as dsavaliacao,
    {{ bronze_inteiro("pronac") }} as pronac,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_texto("tpreadequacao") }} as tpreadequacao,
    {{ bronze_timestamp("dtdistribuicao") }} as dtdistribuicao,
    {{ bronze_inteiro("qtdiasemanalise") }} as qtdiasemanalise,
    {{ bronze_inteiro("idavaliador") }} as idavaliador,
    {{ bronze_texto("nmparecerista") }} as nmparecerista,
    {{ bronze_inteiro("idorgao") }} as idorgao,
    {{ bronze_inteiro("iddistribuirreadequacao") }} as iddistribuirreadequacao,
    _fatia
from {{ source("bronze_sac", "sac__vwpainelreadequacaocoordenadorpareceremanalise") }}
