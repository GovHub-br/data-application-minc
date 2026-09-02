-- Bronze SALIC — sac__vwpainelcoordenadorreadequacaoanalisados.
-- Origem: salic_bronze.sac__vwpainelcoordenadorreadequacaoanalisados, onde tudo chega
-- como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 19 colunas: 13 tipadas, 5 mantidas como texto.
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
    {{ bronze_inteiro("qtdiasdistribuir") }} as qtdiasdistribuir,
    {{ bronze_inteiro("qtdiasavaliar") }} as qtdiasavaliar,
    {{ bronze_inteiro("qttotaldiasavaliar") }} as qttotaldiasavaliar,
    {{ bronze_texto("tpreadequacao") }} as tpreadequacao,
    {{ bronze_inteiro("idtecnicoparecerista") }} as idtecnicoparecerista,
    {{ bronze_texto("nmtecnicoparecerista") }} as nmtecnicoparecerista,
    {{ bronze_inteiro("idorgao") }} as idorgao,
    {{ bronze_texto("sgunidade") }} as sgunidade,
    {{ bronze_texto("idorgaoorigem") }} as idorgaoorigem,
    {{ bronze_inteiro("siencaminhamento") }} as siencaminhamento,
    {{ bronze_inteiro("iddocumentoassinatura") }} as iddocumentoassinatura,
    _fatia
from {{ source("bronze_sac", "sac__vwpainelcoordenadorreadequacaoanalisados") }}
