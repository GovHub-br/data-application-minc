-- Bronze SALIC — sac__vwprogramarouanetpainelresultadofinal.
-- Origem: salic_bronze.sac__vwprogramarouanetpainelresultadofinal, onde tudo chega como
-- texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 18 colunas: 11 tipadas, 6 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idprogramarouanetresultadofinal") }}
    as idprogramarouanetresultadofinal,
    {{ bronze_texto("idprogramarouanet") }} as idprogramarouanet,
    {{ bronze_inteiro("nrproposta") }} as nrproposta,
    {{ bronze_texto("nmproposta") }} as nmproposta,
    {{ bronze_texto("nmregiao") }} as nmregiao,
    {{ bronze_texto("sguf") }} as sguf,
    {{ bronze_inteiro("cdarea") }} as cdarea,
    {{ bronze_numerico("nrpontuacao") }} as nrpontuacao,
    {{ bronze_inteiro("nrrankingpontuacao") }} as nrrankingpontuacao,
    {{ bronze_inteiro("nrrankingfinal") }} as nrrankingfinal,
    {{ bronze_texto("dsresultadofinal") }} as dsresultadofinal,
    {{ bronze_texto("tpselecao") }} as tpselecao,
    {{ bronze_numerico("vlproposta") }} as vlproposta,
    {{ bronze_numerico("vlalocadoregiao") }} as vlalocadoregiao,
    {{ bronze_numerico("vlalocadouf") }} as vlalocadouf,
    {{ bronze_numerico("vlalocadoarea") }} as vlalocadoarea,
    {{ bronze_inteiro("cdfaixa") }} as cdfaixa,
    _fatia
from {{ source("bronze_sac", "sac__vwprogramarouanetpainelresultadofinal") }}
