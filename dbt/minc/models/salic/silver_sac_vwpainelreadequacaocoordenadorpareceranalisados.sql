-- Silver SALIC — silver_sac_vwpainelreadequacaocoordenadorpareceranalisados (ex-sac__vwpainelreadequacaocoordenadorpareceranalisados).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_vwpainelreadequacaocoordenadorpareceranalisados, onde tudo
-- chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 16 colunas: 11 tipadas, 4 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("iddistribuirreadequacao") }} as iddistribuirreadequacao,
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_inteiro("pronac") }} as pronac,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_texto("tpreadequacao") }} as tpreadequacao,
    {{ bronze_timestamp("dtenvio") }} as dtenvio,
    {{ bronze_timestamp("dtdistribuicao") }} as dtdistribuicao,
    {{ bronze_timestamp("dtdevolucao") }} as dtdevolucao,
    {{ bronze_texto("qtdiasdistribuir") }} as qtdiasdistribuir,
    {{ bronze_inteiro("qtdiasavaliar") }} as qtdiasavaliar,
    {{ bronze_inteiro("qttotaldiasavaliar") }} as qttotaldiasavaliar,
    {{ bronze_inteiro("idtecnico") }} as idtecnico,
    {{ bronze_texto("nmparecerista") }} as nmparecerista,
    {{ bronze_inteiro("idreadequacao") }} as idreadequacao,
    {{ bronze_inteiro("idorgao") }} as idorgao,
    _fatia
from {{ source("salic", "bronze_sac_vwpainelreadequacaocoordenadorpareceranalisados") }}
