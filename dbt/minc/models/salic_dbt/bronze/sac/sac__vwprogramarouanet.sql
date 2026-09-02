-- Bronze SALIC — sac__vwprogramarouanet.
-- Origem: salic_bronze.sac__vwprogramarouanet, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 19 colunas: 11 tipadas, 7 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idprogramarouanet") }} as idprogramarouanet,
    {{ bronze_texto("tipologia") }} as tipologia,
    {{ bronze_inteiro("tptipologia") }} as tptipologia,
    {{ bronze_timestamp("dataabertura") }} as dataabertura,
    {{ bronze_timestamp("datafechamento") }} as datafechamento,
    {{ bronze_texto("objetivo") }} as objetivo,
    {{ bronze_texto("qtavaliadores") }} as qtavaliadores,
    {{ bronze_texto("qtpropostas") }} as qtpropostas,
    {{ bronze_texto("tppessoa") }} as tppessoa,
    {{ bronze_texto("pessoa") }} as pessoa,
    {{ bronze_numerico("vlmaximoproposta") }} as vlmaximoproposta,
    {{ bronze_timestamp("datahabilitacao") }} as datahabilitacao,
    {{ bronze_timestamp("dataselecao") }} as dataselecao,
    {{ bronze_timestamp("dataresultado") }} as dataresultado,
    {{ bronze_inteiro("nrpropostaselecionada") }} as nrpropostaselecionada,
    {{ bronze_inteiro("nrpontuacaominima") }} as nrpontuacaominima,
    {{ bronze_numerico("vltotalprograma") }} as vltotalprograma,
    {{ bronze_texto("stestado") }} as stestado,
    _fatia
from {{ source("bronze_sac", "sac__vwprogramarouanet") }}
