-- Bronze SALIC — sac__tbprogramarouanet.
-- Origem: salic_bronze.sac__tbprogramarouanet, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 17 colunas: 15 tipadas, 1 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idprogramarouanet") }} as idprogramarouanet,
    {{ bronze_inteiro("tptipologia") }} as tptipologia,
    {{ bronze_timestamp("dtabertura") }} as dtabertura,
    {{ bronze_timestamp("dtfechamento") }} as dtfechamento,
    {{ bronze_texto("dsobjetivo") }} as dsobjetivo,
    {{ bronze_inteiro("qtavaliadores") }} as qtavaliadores,
    {{ bronze_inteiro("qtpropostas") }} as qtpropostas,
    {{ bronze_booleano("tppessoa") }} as tppessoa,
    {{ bronze_numerico("vlmaximoproposta") }} as vlmaximoproposta,
    {{ bronze_timestamp("dthabilitacao") }} as dthabilitacao,
    {{ bronze_timestamp("dtresultado") }} as dtresultado,
    {{ bronze_booleano("stestado") }} as stestado,
    {{ bronze_timestamp("dtselecao") }} as dtselecao,
    {{ bronze_inteiro("nrpropostaselecionada") }} as nrpropostaselecionada,
    {{ bronze_inteiro("nrpontuacaominima") }} as nrpontuacaominima,
    {{ bronze_numerico("vltotalprograma") }} as vltotalprograma,
    _fatia
from {{ source("bronze_sac", "sac__tbprogramarouanet") }}
