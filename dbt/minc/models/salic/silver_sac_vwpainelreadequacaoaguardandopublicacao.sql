-- Silver SALIC — silver_sac_vwpainelreadequacaoaguardandopublicacao (ex-sac__vwpainelreadequacaoaguardandopublicacao).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_vwpainelreadequacaoaguardandopublicacao, onde tudo chega
-- como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 11 colunas: 5 tipadas, 5 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_inteiro("idreadequacao") }} as idreadequacao,
    {{ bronze_inteiro("pronac") }} as pronac,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_texto("tpreadequacao") }} as tpreadequacao,
    {{ bronze_inteiro("idtecnicoparecerista") }} as idtecnicoparecerista,
    {{ bronze_texto("nmtecnicoparecerista") }} as nmtecnicoparecerista,
    {{ bronze_inteiro("idorgao") }} as idorgao,
    {{ bronze_texto("sgunidade") }} as sgunidade,
    {{ bronze_texto("idorgaoorigem") }} as idorgaoorigem,
    _fatia
from {{ source("salic", "bronze_sac_vwpainelreadequacaoaguardandopublicacao") }}
