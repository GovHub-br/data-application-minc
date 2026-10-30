-- Silver SALIC — silver_sac_vwpainelcoordenadorreadequacaoaguardandoanalise
-- (ex-sac__vwpainelcoordenadorreadequacaoaguardandoanalise).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_vwpainelcoordenadorreadequacaoaguardandoanalise, onde tudo
-- chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 10 colunas: 6 tipadas, 3 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_inteiro("idreadequacao") }} as idreadequacao,
    {{ bronze_texto("pronac") }} as pronac,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_inteiro("idorgao") }} as idorgao,
    {{ bronze_timestamp("dtsolicitacao") }} as dtsolicitacao,
    {{ bronze_texto("tpreadequacao") }} as tpreadequacao,
    {{ bronze_inteiro("qtaguardandodistribuicao") }} as qtaguardandodistribuicao,
    {{ bronze_timestamp("dtenvio") }} as dtenvio,
    _fatia
from {{ source("salic", "bronze_sac_vwpainelcoordenadorreadequacaoaguardandoanalise") }}
