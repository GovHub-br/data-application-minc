-- Silver SALIC — silver_sac_vwpainelcoordenadorreadequacaoemanalise_20180323
-- (ex-sac__vwpainelcoordenadorreadequacaoemanalise_20180323).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_vwpainelcoordenadorreadequacaoemanalise_20180323, onde tudo
-- chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 19 colunas: 9 tipadas, 9 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_inteiro("idreadequacao") }} as idreadequacao,
    {{ bronze_texto("pronac") }} as pronac,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_timestamp("dtsolicitacao") }} as dtsolicitacao,
    {{ bronze_timestamp("dtencaminhamento") }} as dtencaminhamento,
    {{ bronze_inteiro("qtdiasencaminhar") }} as qtdiasencaminhar,
    {{ bronze_texto("tpreadequacao") }} as tpreadequacao,
    {{ bronze_texto("siencaminhamento") }} as siencaminhamento,
    {{ bronze_texto("dsencaminhamento") }} as dsencaminhamento,
    {{ bronze_timestamp("dtdistribuicao") }} as dtdistribuicao,
    {{ bronze_inteiro("qtdiasemanalise") }} as qtdiasemanalise,
    {{ bronze_inteiro("idtecnicoparecerista") }} as idtecnicoparecerista,
    {{ bronze_texto("nmreceptor") }} as nmreceptor,
    {{ bronze_texto("nmtecnicoparecerista") }} as nmtecnicoparecerista,
    {{ bronze_inteiro("idorgao") }} as idorgao,
    {{ bronze_texto("sgunidade") }} as sgunidade,
    {{ bronze_texto("idorgaoorigem") }} as idorgaoorigem,
    _fatia
from {{ source("salic", "bronze_sac_vwpainelcoordenadorreadequacaoemanalise_20180323") }}
