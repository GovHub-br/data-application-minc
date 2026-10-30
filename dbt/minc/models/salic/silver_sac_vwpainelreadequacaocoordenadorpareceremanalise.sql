-- Silver SALIC — silver_sac_vwpainelreadequacaocoordenadorpareceremanalise
-- (ex-sac__vwpainelreadequacaocoordenadorpareceremanalise).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_vwpainelreadequacaocoordenadorpareceremanalise, onde tudo
-- chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 13 colunas: 7 tipadas, 5 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_inteiro("idreadequacao") }} as idreadequacao,
    {{ bronze_texto("dsavaliacao") }} as dsavaliacao,
    {{ bronze_texto("pronac") }} as pronac,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_texto("tpreadequacao") }} as tpreadequacao,
    {{ bronze_timestamp("dtdistribuicao") }} as dtdistribuicao,
    {{ bronze_inteiro("qtdiasemanalise") }} as qtdiasemanalise,
    {{ bronze_inteiro("idavaliador") }} as idavaliador,
    {{ bronze_texto("nmparecerista") }} as nmparecerista,
    {{ bronze_inteiro("idorgao") }} as idorgao,
    {{ bronze_inteiro("iddistribuirreadequacao") }} as iddistribuirreadequacao,
    _fatia
from {{ source("salic", "bronze_sac_vwpainelreadequacaocoordenadorpareceremanalise") }}
