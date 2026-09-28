-- Bronze SALIC — sac__tbprogramarouanetpropostaselecionada.
-- Origem: salic_bronze.sac__tbprogramarouanetpropostaselecionada, onde tudo chega como
-- texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 18 colunas: 14 tipadas, 3 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idprogramarouanetpropostaselecionada") }}
    as idprogramarouanetpropostaselecionada,
    {{ bronze_inteiro("idprogramarouanet") }} as idprogramarouanet,
    {{ bronze_inteiro("idpreprojeto") }} as idpreprojeto,
    {{ bronze_inteiro("iduf") }} as iduf,
    {{ bronze_texto("sguf") }} as sguf,
    {{ bronze_texto("nmregiao") }} as nmregiao,
    {{ bronze_numerico("nrpontuacao") }} as nrpontuacao,
    {{ bronze_numerico("vlproposta") }} as vlproposta,
    {{ bronze_inteiro("nrrankingpontuacao") }} as nrrankingpontuacao,
    {{ bronze_inteiro("nrrankingfinal") }} as nrrankingfinal,
    {{ bronze_booleano("siselecao") }} as siselecao,
    {{ bronze_booleano("siselecaoequidade") }} as siselecaoequidade,
    {{ bronze_numerico("vlalocadoarea") }} as vlalocadoarea,
    {{ bronze_numerico("vlalocadouf") }} as vlalocadouf,
    {{ bronze_numerico("vlalocadoregiao") }} as vlalocadoregiao,
    {{ bronze_texto("cdarea") }} as cdarea,
    {{ bronze_inteiro("cdfaixa") }} as cdfaixa,
    _fatia
from {{ source("bronze_sac", "sac__tbprogramarouanetpropostaselecionada") }}
