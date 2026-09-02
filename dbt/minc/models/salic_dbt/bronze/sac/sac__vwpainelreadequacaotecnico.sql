-- Bronze SALIC — sac__vwpainelreadequacaotecnico.
-- Origem: salic_bronze.sac__vwpainelreadequacaotecnico, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 10 colunas: 7 tipadas, 2 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_inteiro("idreadequacao") }} as idreadequacao,
    {{ bronze_inteiro("pronac") }} as pronac,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_texto("tpreadequacao") }} as tpreadequacao,
    {{ bronze_timestamp("dtdistribuicao") }} as dtdistribuicao,
    {{ bronze_inteiro("qtdiasavaliacao") }} as qtdiasavaliacao,
    {{ bronze_inteiro("idtecnicoparecerista") }} as idtecnicoparecerista,
    {{ bronze_inteiro("idorgao") }} as idorgao,
    _fatia
from {{ source("bronze_sac", "sac__vwpainelreadequacaotecnico") }}
