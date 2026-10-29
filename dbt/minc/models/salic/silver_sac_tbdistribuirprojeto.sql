-- Silver SALIC — silver_sac_tbdistribuirprojeto (ex-sac__tbdistribuirprojeto).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_tbdistribuirprojeto, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 14 colunas: 11 tipadas, 2 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("iddistribuirprojeto") }} as iddistribuirprojeto,
    {{ bronze_texto("tpdistribuicao") }} as tpdistribuicao,
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_inteiro("idunidade") }} as idunidade,
    {{ bronze_timestamp("dtenvio") }} as dtenvio,
    {{ bronze_inteiro("idavaliador") }} as idavaliador,
    {{ bronze_timestamp("dtdistribuicao") }} as dtdistribuicao,
    {{ bronze_timestamp("dtdevolucao") }} as dtdevolucao,
    {{ bronze_texto("dsobservacao") }} as dsobservacao,
    {{ bronze_booleano("stfecharanalise") }} as stfecharanalise,
    {{ bronze_timestamp("dtfechamento") }} as dtfechamento,
    {{ bronze_booleano("stestado") }} as stestado,
    {{ bronze_inteiro("idusuario") }} as idusuario,
    _fatia
from {{ source("salic", "bronze_sac_tbdistribuirprojeto") }}
