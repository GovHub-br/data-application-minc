-- Bronze SALIC — sac__vwpainelcoordenadorreadequacaoanalisados_20180323.
-- Origem: salic_bronze.sac__vwpainelcoordenadorreadequacaoanalisados_20180323, onde
-- tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 17 colunas: 7 tipadas, 9 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_inteiro("idreadequacao") }} as idreadequacao,
    {{ bronze_inteiro("pronac") }} as pronac,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_timestamp("dtenvio") }} as dtenvio,
    {{ bronze_timestamp("dtdistribuicao") }} as dtdistribuicao,
    {{ bronze_timestamp("dtdevolucao") }} as dtdevolucao,
    {{ bronze_texto("qtdiasdistribuir") }} as qtdiasdistribuir,
    {{ bronze_texto("qtdiasavaliar") }} as qtdiasavaliar,
    {{ bronze_texto("qttotaldiasavaliar") }} as qttotaldiasavaliar,
    {{ bronze_texto("tpreadequacao") }} as tpreadequacao,
    {{ bronze_inteiro("idtecnicoparecerista") }} as idtecnicoparecerista,
    {{ bronze_texto("nmtecnicoparecerista") }} as nmtecnicoparecerista,
    {{ bronze_texto("idorgao") }} as idorgao,
    {{ bronze_texto("sgunidade") }} as sgunidade,
    {{ bronze_texto("idorgaoorigem") }} as idorgaoorigem,
    _fatia
from {{ source("bronze_sac", "sac__vwpainelcoordenadorreadequacaoanalisados_20180323") }}
