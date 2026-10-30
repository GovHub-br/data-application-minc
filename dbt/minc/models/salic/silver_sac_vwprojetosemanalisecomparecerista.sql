-- Silver SALIC — silver_sac_vwprojetosemanalisecomparecerista (ex-sac__vwprojetosemanalisecomparecerista).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_vwprojetosemanalisecomparecerista, onde tudo chega como
-- texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 25 colunas: 16 tipadas, 8 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("tpanalise") }} as tpanalise,
    {{ bronze_inteiro("iddistribuirparecer") }} as iddistribuirparecer,
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_inteiro("pronac") }} as pronac,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_inteiro("idproduto") }} as idproduto,
    {{ bronze_inteiro("idorgao") }} as idorgao,
    {{ bronze_inteiro("idorgaoorigem") }} as idorgaoorigem,
    {{ bronze_timestamp("dtenvio") }} as dtenvio,
    {{ bronze_inteiro("idagenteparecerista") }} as idagenteparecerista,
    {{ bronze_texto("nmparecerista") }} as nmparecerista,
    {{ bronze_timestamp("dtdistribuicao") }} as dtdistribuicao,
    {{ bronze_timestamp("dtcorrecao") }} as dtcorrecao,
    {{ bronze_timestamp("dtdevolucao") }} as dtdevolucao,
    {{ bronze_texto("stestado") }} as stestado,
    {{ bronze_texto("stprincipal") }} as stprincipal,
    {{ bronze_texto("sianalise") }} as sianalise,
    {{ bronze_inteiro("nrdiasparecerista") }} as nrdiasparecerista,
    {{ bronze_inteiro("nrdiasdiligencia") }} as nrdiasdiligencia,
    {{ bronze_inteiro("nrdiastotalanalise") }} as nrdiastotalanalise,
    {{ bronze_numerico("vlsaldodoempenho") }} as vlsaldodoempenho,
    {{ bronze_timestamp("dtconcessao") }} as dtconcessao,
    {{ bronze_texto("tpparecerista") }} as tpparecerista,
    {{ bronze_texto("situacao") }} as situacao,
    _fatia
from {{ source("salic", "bronze_sac_vwprojetosemanalisecomparecerista") }}
