-- Silver SALIC — silver_sac_vwpainelcoordreadequacaoaguardandoanalise_20180323 (ex-sac__vwpainelcoordenadorreadequacaoaguardandoanalise_20180323).
-- O nome encurta o da origem para caber nos 63 caracteres do Postgres.
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_vwpainelcoordenadorreadequacaoaguardandoanalise_2018 (nome cortado pelo Postgres), onde tudo chega
-- como texto da ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 9 colunas: 6 tipadas, 2 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_inteiro("idreadequacao") }} as idreadequacao,
    {{ bronze_inteiro("pronac") }} as pronac,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_inteiro("idorgao") }} as idorgao,
    {{ bronze_timestamp("dtsolicitacao") }} as dtsolicitacao,
    {{ bronze_texto("tpreadequacao") }} as tpreadequacao,
    {{ bronze_inteiro("qtaguardandodistribuicao") }} as qtaguardandodistribuicao,
    _fatia
from {{ source("salic", "bronze_sac_vwpainelcoordenadorreadequacaoaguardandoanalise_20180323") }}
