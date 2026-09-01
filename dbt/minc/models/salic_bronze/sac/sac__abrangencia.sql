-- Bronze SALIC — sac__abrangencia.
-- Origem: salic_bronze.sac__abrangencia, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 12 colunas: 9 tipadas, 2 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idabrangencia") }} as idabrangencia,
    {{ bronze_inteiro("idprojeto") }} as idprojeto,
    {{ bronze_inteiro("idpais") }} as idpais,
    {{ bronze_inteiro("iduf") }} as iduf,
    {{ bronze_inteiro("idmunicipioibge") }} as idmunicipioibge,
    {{ bronze_inteiro("usuario") }} as usuario,
    {{ bronze_booleano("stabrangencia") }} as stabrangencia,
    {{ bronze_texto("siabrangencia") }} as siabrangencia,
    {{ bronze_texto("dsjustificativa") }} as dsjustificativa,
    {{ bronze_timestamp("dtiniciorealizacao") }} as dtiniciorealizacao,
    {{ bronze_timestamp("dtfimrealizacao") }} as dtfimrealizacao,
    _fatia
from {{ source("bronze_sac", "sac__abrangencia") }}
